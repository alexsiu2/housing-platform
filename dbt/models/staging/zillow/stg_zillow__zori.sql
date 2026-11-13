-- One row per zip per month, cleanly typed.
select
    zip,
    cast(month as date) as month,
    cast(rent_index as double) as rent_index,
    region_id,
    size_rank,
    state,
    city,
    metro,
    county_name,
    cast(ingested_at as timestamptz) as ingested_at,
    source_file,
    run_id
from {{ source('bronze', 'zori_raw') }}
