-- BLS CES total nonfarm employment (NSA, thousands), one row per series-month.
-- For metro series the CES area code is the CBSA code.
select
    series_id,
    state_fips,
    area_code as cbsa_code,
    cast(month as date) as month,
    cast(employment_thousands as double) as employment_thousands,
    is_preliminary,
    cast(ingested_at as timestamptz) as ingested_at,
    source_file,
    run_id
from {{ source('bronze', 'bls_ces_raw') }}
