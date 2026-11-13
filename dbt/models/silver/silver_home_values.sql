{#-
    Zillow ZHVI (typical home value, mid tier, $) by zip and month, with
    1-year growth and the zip's primary metro (CBSA) from the HUD crosswalk.
    cbsa_code is null for zips outside any metro.
-#}
with zhvi as (
    select * from {{ ref('stg_zillow__zhvi') }}
),

primary_cbsa as (
    select zip, cbsa_code
    from {{ ref('silver_zip_cbsa_xwalk') }}
    where is_primary
)

select
    cur.zip,
    cur.month,
    cur.home_value,
    prior.home_value as home_value_12mo_ago,
    round((cur.home_value / nullif(prior.home_value, 0) - 1) * 100, 2)
        as home_value_growth_1yr_pct,
    primary_cbsa.cbsa_code,
    cur.state,
    cur.city,
    cur.metro,
    cur.county_name
from zhvi as cur
left join zhvi as prior
    on prior.zip = cur.zip
    and prior.month = cast(cur.month - interval 12 month as date)
left join primary_cbsa on primary_cbsa.zip = cur.zip
