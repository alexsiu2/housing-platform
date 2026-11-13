-- ACS 5-year estimates, one row per (zcta, variable, vintage_year).
-- value is null where Census suppressed the estimate (is_missing).
select
    zcta,
    variable,
    variable_name,
    cast(vintage_year as integer) as vintage_year,
    cast(value as double) as value,
    is_missing,
    cast(ingested_at as timestamptz) as ingested_at,
    source_file,
    run_id
from {{ source('bronze', 'census_acs_raw') }}
