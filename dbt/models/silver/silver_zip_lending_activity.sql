{#-
    HMDA home-purchase lending by zip and year (Spark -> dbt, design §6.3),
    with year-over-year change in purchase originations. Counts are estimates
    (tract -> zip allocation by residential share), so tiny zips are noisy.
-#}
with lending as (
    select * from {{ ref('stg_spark__zip_lending_activity') }}
)

select
    cur.zip,
    cur.activity_year,
    cur.purchase_loans_est,
    cur.investor_purchase_loans_est,
    cur.investor_purchase_share,
    cur.second_home_purchase_loans_est,
    cur.median_loan_amount_est,
    round(
        (cur.purchase_loans_est / nullif(prior.purchase_loans_est, 0) - 1) * 100, 2
    ) as purchase_loan_count_yoy_pct,
    cur.tracts,
    cur.xwalk_vintage
from lending as cur
left join lending as prior
    on prior.zip = cur.zip
    and prior.activity_year = cur.activity_year - 1
