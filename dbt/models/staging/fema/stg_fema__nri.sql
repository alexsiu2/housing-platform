-- FEMA NRI expected annual building loss by tract (newest release only).
with ranked as (
    select *, max(nri_release) over () as newest
    from {{ source('bronze', 'fema_nri_raw') }}
)

select
    tract,
    county_fips,
    nri_release,
    cast(building_value as double) as building_value,
    cast(eal_building as double) as eal_building,
    cast(eal_building_hurricane as double) as eal_building_hurricane,
    cast(eal_building_coastal_flooding as double) as eal_building_coastal_flooding,
    cast(eal_building_inland_flooding as double) as eal_building_inland_flooding,
    cast(eal_building_wildfire as double) as eal_building_wildfire,
    cast(eal_building_tornado as double) as eal_building_tornado,
    cast(eal_building_strong_wind as double) as eal_building_strong_wind,
    cast(eal_building_hail as double) as eal_building_hail,
    cast(eal_building_earthquake as double) as eal_building_earthquake,
    cast(eal_building_winter_weather as double) as eal_building_winter_weather,
    cast(eal_building_landslide as double) as eal_building_landslide
from ranked
where nri_release = newest
