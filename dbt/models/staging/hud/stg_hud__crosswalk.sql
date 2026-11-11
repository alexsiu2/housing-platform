-- HUD USPS crosswalk rows, all vintages. geoid is a CBSA code for zip-cbsa
-- and an 11-digit census tract for tract-zip. HUD uses 99999 for "no CBSA".
select
    crosswalk_type,
    vintage,
    cast(year as integer) as year,
    cast(quarter as integer) as quarter,
    zip,
    geoid,
    cast(res_ratio as double) as res_ratio,
    cast(bus_ratio as double) as bus_ratio,
    cast(oth_ratio as double) as oth_ratio,
    cast(tot_ratio as double) as tot_ratio,
    city,
    state,
    cast(ingested_at as timestamptz) as ingested_at,
    source_file,
    run_id
from {{ source('bronze', 'hud_crosswalk_raw') }}
