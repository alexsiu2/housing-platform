{#-
    Zillow ZORI (typical market rent, $/month) by zip and month, with 1-year
    growth. Growth compares against exactly 12 months earlier, so a gap in a
    zip's history gives null growth rather than a wrong number.
-#}
with zori as (
    select * from {{ ref('stg_zillow__zori') }}
)

select
    cur.zip,
    cur.month,
    cur.rent_index,
    prior.rent_index as rent_index_12mo_ago,
    round((cur.rent_index / nullif(prior.rent_index, 0) - 1) * 100, 2)
        as rent_growth_1yr_pct,
    cur.state,
    cur.city,
    cur.metro,
    cur.county_name
from zori as cur
left join zori as prior
    on prior.zip = cur.zip
    and prior.month = cast(cur.month - interval 12 month as date)
