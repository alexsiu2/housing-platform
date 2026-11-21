import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "dags"))

import dbt_selection as sel  # noqa: E402


def test_fema_trigger_builds_only_hazard_downstream():
    assert sel.selector_for(["bronze.fema_nri_raw"]) == "source:bronze.fema_nri_raw+"


def test_zillow_ready_covers_both_tables():
    assert sel.selector_for(["bronze.zillow_ready"]) == (
        "source:bronze.zori_raw+ source:bronze.zhvi_raw+"
    )


def test_several_triggers_are_combined_without_duplicates():
    out = sel.selector_for(
        ["bronze.fred_series_raw", "bronze.fema_nri_raw", "bronze.fema_nri_raw"]
    )
    assert out == "source:bronze.fema_nri_raw+ source:bronze.fred_series_raw+"


def test_manual_or_unknown_trigger_falls_back_to_full_build():
    assert sel.selector_for([]) == sel.FULL_BUILD
    assert sel.selector_for(None) == sel.FULL_BUILD
    assert sel.selector_for(["bronze.something_new"]) == sel.FULL_BUILD


def test_every_dbt_trigger_asset_has_a_selector():
    """A new ingest wired into dbt_build must get a selector here too."""
    src = (Path(__file__).resolve().parents[1] / "dags" / "rpof_assets.py").read_text()
    block = src.split("DBT_TRIGGERS = [", 1)[1].split("]", 1)[0]
    names = {
        "BRONZE_FRED": "bronze.fred_series_raw",
        "ZILLOW_READY": "bronze.zillow_ready",
        "BRONZE_HUD_CROSSWALK": "bronze.hud_crosswalk_raw",
        "BRONZE_BLS_CES": "bronze.bls_ces_raw",
        "BRONZE_CENSUS_ACS": "bronze.census_acs_raw",
        "SILVER_SPARK_LENDING": "silver_spark.zip_lending_activity",
        "BRONZE_FEMA_NRI": "bronze.fema_nri_raw",
        "BRONZE_GDELT": "bronze.gdelt_housing_articles_raw",
    }
    triggers = [t.strip() for t in block.split(",") if t.strip()]
    assert set(triggers) == set(names), "update this test's name map for new assets"
    # Every selector is a trigger, except the streamed news (built daily).
    assert {names[t] for t in triggers} == set(sel.ASSET_SELECTORS) - {
        "bronze.news_events_raw"
    }


def test_streamed_news_is_daily_not_a_trigger():
    """Every-15-minute news must not trigger dbt (and rescoring) each time."""
    src = (Path(__file__).resolve().parents[1] / "dags" / "rpof_assets.py").read_text()
    block = src.split("DBT_TRIGGERS = [", 1)[1].split("]", 1)[0]
    assert "BRONZE_NEWS" not in block
    assert sel.DAILY_SELECT == "source:bronze.news_events_raw+"
