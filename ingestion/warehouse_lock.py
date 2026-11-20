"""
One writer at a time for the DuckDB warehouse (data/rpof.duckdb).

DuckDB locks its file, but that lock does not work across Docker Desktop's
file sharing between macOS and the Linux VM: a `dbt build` on the host and
Airflow's dbt_build in a container both opened the warehouse for writing and
corrupted it ("Serialization Error: field id mismatch"). This lock is a
directory next to the warehouse; creating a directory is atomic on both sides
of the share, so exactly one writer wins and the others wait.

    with warehouse_lock("compute_scores"):
        ...

    python -m ingestion.warehouse_lock run -- dbt build      # wrap a command
"""

import argparse
import json
import logging
import os
import shutil
import socket
import subprocess
import sys
import time
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path

logger = logging.getLogger(__name__)

DEFAULT_WAIT_SECONDS = 1800
STALE_AFTER_SECONDS = 3 * 3600  # longer than any build; then assume a crashed holder
POLL_SECONDS = 5


class WarehouseLockTimeout(TimeoutError):
    """Another writer held the warehouse lock for the whole wait."""


def lock_path() -> Path:
    from scoring.score import duckdb_path

    return Path(duckdb_path()).with_name(".warehouse.lock")


def _owner(path: Path) -> dict:
    try:
        return json.loads((path / "owner.json").read_text())
    except (OSError, ValueError):
        return {}


def _age_seconds(path: Path) -> float:
    try:
        return time.time() - path.stat().st_mtime
    except OSError:
        return 0.0


@contextmanager
def warehouse_lock(
    purpose: str,
    wait_seconds: float = DEFAULT_WAIT_SECONDS,
    path: Path | None = None,
    stale_after: float = STALE_AFTER_SECONDS,
    poll: float = POLL_SECONDS,
):
    path = Path(path or lock_path())
    path.parent.mkdir(parents=True, exist_ok=True)
    deadline = time.monotonic() + wait_seconds
    while True:
        try:
            path.mkdir()
            break
        except FileExistsError:
            if _age_seconds(path) > stale_after:
                logger.warning("Breaking stale warehouse lock held by %s", _owner(path))
                shutil.rmtree(path, ignore_errors=True)
                continue
            if time.monotonic() >= deadline:
                raise WarehouseLockTimeout(
                    f"Warehouse is locked by {_owner(path) or 'another writer'}; "
                    f"waited {wait_seconds:.0f}s"
                ) from None
            logger.info("Warehouse locked by %s; waiting", _owner(path))
            time.sleep(poll)

    (path / "owner.json").write_text(
        json.dumps(
            {
                "purpose": purpose,
                "host": socket.gethostname(),
                "pid": os.getpid(),
                "since": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            }
        )
    )
    try:
        yield
    finally:
        shutil.rmtree(path, ignore_errors=True)


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Run a command holding the warehouse lock")
    sub = p.add_subparsers(dest="action", required=True)
    run = sub.add_parser("run")
    run.add_argument("--wait", type=float, default=DEFAULT_WAIT_SECONDS)
    run.add_argument("cmd", nargs=argparse.REMAINDER)
    args = p.parse_args(argv)
    cmd = args.cmd[1:] if args.cmd[:1] == ["--"] else args.cmd
    if not cmd:
        p.error("no command given")
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s %(message)s")
    with warehouse_lock(" ".join(cmd)[:120], wait_seconds=args.wait):
        return subprocess.call(cmd)


if __name__ == "__main__":
    sys.exit(main())
