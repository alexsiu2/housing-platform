"""
Idempotent loads into Bronze Delta tables.

Every ingest task ends with merge_to_delta(): rows are upserted on the
source's natural key, so re-running a task or backfilling an overlapping
date range never creates duplicates.
"""

import logging
import uuid
from datetime import datetime, timezone

import pandas as pd
import pyarrow as pa
from deltalake import DeltaTable, write_deltalake

logger = logging.getLogger(__name__)


def _quote(column: str) -> str:
    return f"`{column}`"


def _decode_dictionaries(table: pa.Table) -> pa.Table:
    """Cast dictionary-encoded columns (pandas categoricals) to their values."""
    for i, field in enumerate(table.schema):
        if pa.types.is_dictionary(field.type):
            table = table.set_column(
                i, field.name, table.column(i).cast(field.type.value_type)
            )
    return table


def merge_to_delta(
    df: pd.DataFrame,
    table_uri: str,
    keys: list[str],
    storage_options: dict[str, str] | None = None,
    partition_by: list[str] | None = None,
    run_id: str | None = None,
    target_filter: str | None = None,
) -> dict:
    """
    Upsert a DataFrame into a Delta table on the given key columns.

    - Creates the table on first write.
    - Adds a `run_id` column (one per call) so every row is traceable to a load.
    - If the input has duplicate keys, the last occurrence wins.
    - `target_filter` (SQL on the target alias `t`, e.g. "t.year >= 2024")
      limits which existing rows the MERGE scans. Only use it when every
      source row satisfies it, or matching rows outside it would be duplicated.
    - Categorical (dictionary) columns are written as plain strings.

    Returns a dict of load metrics: rows_in, rows_inserted, rows_updated,
    run_id, created_table.
    """
    if not keys:
        raise ValueError("keys must contain at least one column")
    missing = [k for k in keys if k not in df.columns]
    if missing:
        raise KeyError(f"Key columns not in DataFrame: {missing}")
    if df[keys].isna().any().any():
        raise ValueError(f"Key columns {keys} contain nulls")

    run_id = run_id or uuid.uuid4().hex
    df = df.drop_duplicates(subset=keys, keep="last").copy()
    df["run_id"] = run_id
    data = _decode_dictionaries(pa.Table.from_pandas(df, preserve_index=False))

    started = datetime.now(timezone.utc)

    if not DeltaTable.is_deltatable(table_uri, storage_options=storage_options):
        write_deltalake(
            table_uri,
            data,
            mode="error",
            partition_by=partition_by,
            storage_options=storage_options,
        )
        metrics = {
            "rows_in": len(df),
            "rows_inserted": len(df),
            "rows_updated": 0,
            "created_table": True,
        }
    else:
        predicate = " AND ".join(f"t.{_quote(k)} = s.{_quote(k)}" for k in keys)
        if target_filter:
            predicate = f"({predicate}) AND ({target_filter})"
        result = (
            DeltaTable(table_uri, storage_options=storage_options)
            .merge(
                source=data,
                predicate=predicate,
                source_alias="s",
                target_alias="t",
            )
            .when_matched_update_all()
            .when_not_matched_insert_all()
            .execute()
        )
        metrics = {
            "rows_in": len(df),
            "rows_inserted": result.get("num_target_rows_inserted", 0),
            "rows_updated": result.get("num_target_rows_updated", 0),
            "created_table": False,
        }

    metrics["run_id"] = run_id
    metrics["duration_s"] = (datetime.now(timezone.utc) - started).total_seconds()
    logger.info("Merged into %s: %s", table_uri, metrics)
    return metrics
