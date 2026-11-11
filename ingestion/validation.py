"""
Ingestion-time data quality checks (Great Expectations).

Each source has a suite that runs on the parsed DataFrame *before* it is
merged into Bronze. A failing suite raises DataQualityError, which fails the
Airflow task, so bad data never lands silently.
"""

import logging

import great_expectations as gx
import pandas as pd

logger = logging.getLogger(__name__)


class DataQualityError(Exception):
    """Raised when a Great Expectations suite fails."""


def _fred_suite(known_series: list[str]) -> gx.ExpectationSuite:
    e = gx.expectations
    return gx.ExpectationSuite(
        name="fred_observations",
        expectations=[
            e.ExpectTableColumnsToMatchSet(
                column_set=[
                    "date",
                    "series_id",
                    "series_name",
                    "value",
                    "ingested_at",
                    "source_file",
                ],
                exact_match=False,
            ),
            e.ExpectTableRowCountToBeBetween(min_value=1),
            e.ExpectColumnValuesToNotBeNull(column="date"),
            e.ExpectColumnValuesToNotBeNull(column="series_id"),
            e.ExpectColumnValuesToNotBeNull(column="value"),
            e.ExpectColumnValuesToBeInSet(column="series_id", value_set=known_series),
            e.ExpectCompoundColumnsToBeUnique(column_list=["series_id", "date"]),
            # Every tracked series is a level/rate that is never negative.
            e.ExpectColumnValuesToBeBetween(column="value", min_value=0),
            e.ExpectColumnValuesToBeBetween(
                column="date",
                min_value=pd.Timestamp("1900-01-01"),
                max_value=pd.Timestamp.now().normalize() + pd.DateOffset(days=1),
            ),
        ],
    )


def _run_suite(df: pd.DataFrame, suite: gx.ExpectationSuite) -> dict:
    context = gx.get_context(mode="ephemeral")
    batch = (
        context.data_sources.add_pandas("ingestion")
        .add_dataframe_asset(suite.name)
        .add_batch_definition_whole_dataframe("whole")
        .get_batch(batch_parameters={"dataframe": df})
    )
    result = batch.validate(suite)

    failures = [
        f"{r.expectation_config.type}"
        f"({r.expectation_config.kwargs.get('column', '')})"
        for r in result.results
        if not r.success
    ]
    summary = {
        "suite": suite.name,
        "success": result.success,
        "evaluated": len(result.results),
        "failed": failures,
    }
    logger.info("Validation %s", summary)
    return summary


def validate_fred_observations(df: pd.DataFrame, known_series: list[str]) -> dict:
    """Validate parsed FRED rows; raise DataQualityError if any check fails."""
    summary = _run_suite(df, _fred_suite(known_series))
    if not summary["success"]:
        raise DataQualityError(
            f"{summary['suite']} failed {len(summary['failed'])} "
            f"expectation(s): {', '.join(summary['failed'])}"
        )
    return summary
