-- One row per zip per month, cleanly typed.
select
    zip,
    cast(month as date) as month,
    cast(home_value as double) as home_value,
    region_id,
    size_rank,
    state,
    city,
    metro,
    county_name,
    cast(ingested_at as timestamptz) as ingested_at,
    source_file,
    run_id
from {{ source('bronze', 'zhvi_raw') }}
