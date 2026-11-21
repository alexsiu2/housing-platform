"""
Load FEMA National Risk Index tracts into bronze.fema_nri_raw through the same
pipeline as the yearly `ingest_fema_nri` DAG (~43 sequential API pages).

    python scripts/load_fema_nri.py
    make load-nri
"""

import logging
import sys
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from ingestion.nri_pipeline import extract_to_raw, load_raw_to_bronze  # noqa: E402


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s %(message)s")
    key = extract_to_raw()
    m = load_raw_to_bronze(key, run_id=f"manual_{date.today():%Y%m%d}")
    print(
        f"FEMA NRI release {m['nri_release']}: {m['tracts']:,} tracts "
        f"({m['rows_inserted']:,} new) | "
        f"GE {m['validation']['evaluated']} checks passed"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
