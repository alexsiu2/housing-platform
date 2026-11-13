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
    # Progress bars flood Airflow task logs with carriage-return noise.
    context.variables.progress_bars = gx.data_context.types.base.ProgressBarsConfig(
        globally=False
    )
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


def _raise_on_failure(summary: dict) -> dict:
    if not summary["success"]:
        raise DataQualityError(
            f"{summary['suite']} failed {len(summary['failed'])} "
            f"expectation(s): {', '.join(summary['failed'])}"
        )
    return summary


def validate_fred_observations(df: pd.DataFrame, known_series: list[str]) -> dict:
    """Validate parsed FRED rows; raise DataQualityError if any check fails."""
    return _raise_on_failure(_run_suite(df, _fred_suite(known_series)))


def _not_in_future(column: str):
    return gx.expectations.ExpectColumnValuesToBeBetween(
        column=column,
        min_value=pd.Timestamp("1990-01-01"),
        max_value=pd.Timestamp.now().normalize() + pd.DateOffset(days=1),
    )


def _zillow_suite(dataset: str, value_column: str) -> gx.ExpectationSuite:
    e = gx.expectations
    return gx.ExpectationSuite(
        name=f"zillow_{dataset}",
        expectations=[
            e.ExpectTableColumnsToMatchSet(
                column_set=["zip", "month", "year", value_column, "source_file"],
                exact_match=False,
            ),
            e.ExpectTableRowCountToBeBetween(min_value=1),
            e.ExpectColumnValuesToNotBeNull(column="zip"),
            e.ExpectColumnValuesToNotBeNull(column="month"),
            e.ExpectColumnValuesToNotBeNull(column=value_column),
            e.ExpectColumnValuesToMatchRegex(column="zip", regex=r"^\d{5}$"),
            e.ExpectColumnValuesToBeInSet(column="region_type", value_set=["zip"]),
            e.ExpectCompoundColumnsToBeUnique(column_list=["zip", "month"]),
            e.ExpectColumnValuesToBeBetween(
                column=value_column, min_value=0, strict_min=True
            ),
            _not_in_future("month"),
        ],
    )


def validate_zillow(df: pd.DataFrame, dataset: str, value_column: str) -> dict:
    """Validate melted Zillow rows; raise DataQualityError if any check fails."""
    return _raise_on_failure(_run_suite(df, _zillow_suite(dataset, value_column)))


_GEOID_PATTERNS = {"zip-cbsa": r"^\d{5}$", "tract-zip": r"^\d{11}$"}


def _hud_suite(crosswalk: str) -> gx.ExpectationSuite:
    e = gx.expectations
    ratio_checks = [
        e.ExpectColumnValuesToBeBetween(column=c, min_value=0, max_value=1)
        for c in ["res_ratio", "bus_ratio", "oth_ratio", "tot_ratio"]
    ]
    return gx.ExpectationSuite(
        name=f"hud_{crosswalk.replace('-', '_')}",
        expectations=[
            e.ExpectTableRowCountToBeBetween(min_value=1000),
            e.ExpectColumnValuesToNotBeNull(column="zip"),
            e.ExpectColumnValuesToNotBeNull(column="geoid"),
            e.ExpectColumnValuesToNotBeNull(column="res_ratio"),
            e.ExpectColumnValuesToMatchRegex(column="zip", regex=r"^\d{5}$"),
            e.ExpectColumnValuesToMatchRegex(
                column="geoid", regex=_GEOID_PATTERNS[crosswalk]
            ),
            e.ExpectColumnValuesToBeInSet(
                column="crosswalk_type", value_set=[crosswalk]
            ),
            e.ExpectCompoundColumnsToBeUnique(column_list=["zip", "geoid"]),
            *ratio_checks,
        ],
    )


def validate_hud_crosswalk(df: pd.DataFrame, crosswalk: str) -> dict:
    """Validate a parsed HUD crosswalk; raise DataQualityError on failure."""
    return _raise_on_failure(_run_suite(df, _hud_suite(crosswalk)))
