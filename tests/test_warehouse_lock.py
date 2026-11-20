import json
import sys
import threading
import time

import pytest

from ingestion import warehouse_lock as wl


def test_lock_creates_and_releases_with_owner(tmp_path):
    lock = tmp_path / ".warehouse.lock"
    with wl.warehouse_lock("dbt build", path=lock):
        assert lock.is_dir()
        owner = json.loads((lock / "owner.json").read_text())
        assert owner["purpose"] == "dbt build" and "host" in owner
    assert not lock.exists()


def test_second_writer_waits_for_the_first(tmp_path):
    lock = tmp_path / ".warehouse.lock"
    order = []

    def first():
        with wl.warehouse_lock("first", path=lock):
            order.append("first-in")
            time.sleep(0.3)
            order.append("first-out")

    t = threading.Thread(target=first)
    t.start()
    time.sleep(0.05)
    with wl.warehouse_lock("second", path=lock, wait_seconds=5, poll=0.05):
        order.append("second-in")
    t.join()
    assert order == ["first-in", "first-out", "second-in"]


def test_times_out_naming_the_holder(tmp_path):
    lock = tmp_path / ".warehouse.lock"
    with wl.warehouse_lock("airflow dbt_build", path=lock):
        with pytest.raises(wl.WarehouseLockTimeout, match="airflow dbt_build"):
            with wl.warehouse_lock(
                "make score", path=lock, wait_seconds=0.2, poll=0.05
            ):
                pass


def test_breaks_a_stale_lock(tmp_path):
    lock = tmp_path / ".warehouse.lock"
    lock.mkdir()  # crashed holder
    with wl.warehouse_lock("x", path=lock, stale_after=0, wait_seconds=1, poll=0.05):
        assert lock.is_dir()


def test_released_even_when_the_body_fails(tmp_path):
    lock = tmp_path / ".warehouse.lock"
    with pytest.raises(RuntimeError):
        with wl.warehouse_lock("x", path=lock):
            raise RuntimeError("dbt failed")
    assert not lock.exists()


def test_cli_runs_command_and_returns_its_exit_code(tmp_path, monkeypatch):
    monkeypatch.setattr(wl, "lock_path", lambda: tmp_path / ".warehouse.lock")
    code = wl.main(["run", "--", sys.executable, "-c", "raise SystemExit(3)"])
    assert code == 3
    assert not (tmp_path / ".warehouse.lock").exists()
