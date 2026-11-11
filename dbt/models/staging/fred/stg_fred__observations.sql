-- One row per FRED series per observation date, cleanly typed.
select
    series_id,
    series_name,
    cast("date" as date) as observation_date,
    cast(value as double) as value,
    cast(ingested_at as timestamptz) as ingested_at,
    source_file,
    run_id
from {{ source('bronze', 'fred_series_raw') }}
