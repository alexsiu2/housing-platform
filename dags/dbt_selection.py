"""
Which dbt nodes to build for the Bronze assets that triggered a dbt_build run.

Rebuilding everything on every trigger reloaded every Bronze source (e.g.
the 6.5M-row ZHVI table) even when only FEMA's small table had changed, and
that full build got OOM-killed in the shared Docker VM. Building only
downstream of what changed is faster and far lighter. Gold models still
read the other Silver tables, which are already in the warehouse.

No Airflow imports, so it can be unit-tested.
"""

# Asset name -> dbt selector for everything downstream of it.
ASSET_SELECTORS = {
    "bronze.fred_series_raw": "source:bronze.fred_series_raw+",
    "bronze.zillow_ready": "source:bronze.zori_raw+ source:bronze.zhvi_raw+",
    "bronze.hud_crosswalk_raw": "source:bronze.hud_crosswalk_raw+",
    "bronze.bls_ces_raw": "source:bronze.bls_ces_raw+",
    "bronze.census_acs_raw": "source:bronze.census_acs_raw+",
    "bronze.fema_nri_raw": "source:bronze.fema_nri_raw+",
    "bronze.gdelt_housing_articles_raw": "source:bronze.gdelt_housing_articles_raw+",
    "silver_spark.zip_lending_activity": "source:silver_spark.zip_lending_activity+",
    "bronze.news_events_raw": "source:bronze.news_events_raw+",
}

# dbt_build's daily scheduled run: only the streamed news, which changes every
# 15 minutes but is not an asset trigger (dags/stream_news_to_bronze.py).
DAILY_SELECT = ASSET_SELECTORS["bronze.news_events_raw"]

FULL_BUILD = "source:bronze+ source:silver_spark+"


def selector_for(triggering_asset_names: list[str] | None) -> str:
    """
    Union of the selectors for the triggering assets. No triggers (a manual
    run) or any unknown asset -> the full build, so nothing is ever skipped.
    """
    names = sorted(set(triggering_asset_names or []))
    if not names or any(n not in ASSET_SELECTORS for n in names):
        return FULL_BUILD
    parts = []
    for name in names:
        for sel in ASSET_SELECTORS[name].split():
            if sel not in parts:
                parts.append(sel)
    return " ".join(parts)
