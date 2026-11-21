{#-
    Natural-hazard exposure by zip from FEMA's National Risk Index: expected
    annual *building* loss as a % of building value, total and per peril.
    Tracts are allocated to zips by residential share (HUD tract->zip, latest
    vintage; NRI uses 2020 tracts). Shown as context, not scored (owner's
    decision: high-scoring zips do not skew toward hazard).

    Deliberately not FEMA's composite RISK_SCORE, which also folds in social
    vulnerability and community resilience (income/demographics), not
    property risk.
-#}
with allocated as (
    select
        x.zip,
        any_value(n.nri_release) as nri_release,
        count(distinct n.tract) as tracts,
        sum(n.building_value * x.res_ratio) as value_w,
        sum(n.eal_building * x.res_ratio) as loss_total,
        sum(n.eal_building_hurricane * x.res_ratio) as loss_hurricane,
        sum(n.eal_building_coastal_flooding * x.res_ratio) as loss_coastal_flooding,
        sum(n.eal_building_inland_flooding * x.res_ratio) as loss_inland_flooding,
        sum(n.eal_building_wildfire * x.res_ratio) as loss_wildfire,
        sum(n.eal_building_tornado * x.res_ratio) as loss_tornado,
        sum(n.eal_building_strong_wind * x.res_ratio) as loss_strong_wind,
        sum(n.eal_building_hail * x.res_ratio) as loss_hail,
        sum(n.eal_building_earthquake * x.res_ratio) as loss_earthquake,
        sum(n.eal_building_winter_weather * x.res_ratio) as loss_winter_weather,
        sum(n.eal_building_landslide * x.res_ratio) as loss_landslide
    from {{ ref('silver_tract_zip_xwalk') }} as x
    inner join {{ ref('stg_fema__nri') }} as n on n.tract = x.tract
    group by x.zip
),

rated as (
    select
        zip,
        nri_release,
        tracts,
        round(loss_total / nullif(value_w, 0) * 100, 4) as eal_rate_pct,
    round(loss_hurricane / nullif(value_w, 0) * 100, 4) as eal_rate_hurricane_pct,
    round(loss_coastal_flooding / nullif(value_w, 0) * 100, 4) as eal_rate_coastal_flooding_pct,
    round(loss_inland_flooding / nullif(value_w, 0) * 100, 4) as eal_rate_inland_flooding_pct,
    round(loss_wildfire / nullif(value_w, 0) * 100, 4) as eal_rate_wildfire_pct,
    round(loss_tornado / nullif(value_w, 0) * 100, 4) as eal_rate_tornado_pct,
    round(loss_strong_wind / nullif(value_w, 0) * 100, 4) as eal_rate_strong_wind_pct,
    round(loss_hail / nullif(value_w, 0) * 100, 4) as eal_rate_hail_pct,
    round(loss_earthquake / nullif(value_w, 0) * 100, 4) as eal_rate_earthquake_pct,
    round(loss_winter_weather / nullif(value_w, 0) * 100, 4) as eal_rate_winter_weather_pct,
    round(loss_landslide / nullif(value_w, 0) * 100, 4) as eal_rate_landslide_pct,
        case
            when loss_hurricane = greatest(loss_hurricane, loss_coastal_flooding, loss_inland_flooding, loss_wildfire, loss_tornado, loss_strong_wind, loss_hail, loss_earthquake, loss_winter_weather, loss_landslide) then 'hurricane'
            when loss_coastal_flooding = greatest(loss_hurricane, loss_coastal_flooding, loss_inland_flooding, loss_wildfire, loss_tornado, loss_strong_wind, loss_hail, loss_earthquake, loss_winter_weather, loss_landslide) then 'coastal_flooding'
            when loss_inland_flooding = greatest(loss_hurricane, loss_coastal_flooding, loss_inland_flooding, loss_wildfire, loss_tornado, loss_strong_wind, loss_hail, loss_earthquake, loss_winter_weather, loss_landslide) then 'inland_flooding'
            when loss_wildfire = greatest(loss_hurricane, loss_coastal_flooding, loss_inland_flooding, loss_wildfire, loss_tornado, loss_strong_wind, loss_hail, loss_earthquake, loss_winter_weather, loss_landslide) then 'wildfire'
            when loss_tornado = greatest(loss_hurricane, loss_coastal_flooding, loss_inland_flooding, loss_wildfire, loss_tornado, loss_strong_wind, loss_hail, loss_earthquake, loss_winter_weather, loss_landslide) then 'tornado'
            when loss_strong_wind = greatest(loss_hurricane, loss_coastal_flooding, loss_inland_flooding, loss_wildfire, loss_tornado, loss_strong_wind, loss_hail, loss_earthquake, loss_winter_weather, loss_landslide) then 'strong_wind'
            when loss_hail = greatest(loss_hurricane, loss_coastal_flooding, loss_inland_flooding, loss_wildfire, loss_tornado, loss_strong_wind, loss_hail, loss_earthquake, loss_winter_weather, loss_landslide) then 'hail'
            when loss_earthquake = greatest(loss_hurricane, loss_coastal_flooding, loss_inland_flooding, loss_wildfire, loss_tornado, loss_strong_wind, loss_hail, loss_earthquake, loss_winter_weather, loss_landslide) then 'earthquake'
            when loss_winter_weather = greatest(loss_hurricane, loss_coastal_flooding, loss_inland_flooding, loss_wildfire, loss_tornado, loss_strong_wind, loss_hail, loss_earthquake, loss_winter_weather, loss_landslide) then 'winter_weather'
            when loss_landslide = greatest(loss_hurricane, loss_coastal_flooding, loss_inland_flooding, loss_wildfire, loss_tornado, loss_strong_wind, loss_hail, loss_earthquake, loss_winter_weather, loss_landslide) then 'landslide'
        end as top_peril,
        round(greatest(loss_hurricane, loss_coastal_flooding, loss_inland_flooding, loss_wildfire, loss_tornado, loss_strong_wind, loss_hail, loss_earthquake, loss_winter_weather, loss_landslide) / nullif(loss_total, 0), 3)
            as top_peril_share
    from allocated
    where value_w > 0
)

select
    *,
    round(percent_rank() over (order by eal_rate_pct) * 100, 1) as hazard_percentile,
    percent_rank() over (order by eal_rate_pct) >= 0.9 as is_high_hazard
from rated
