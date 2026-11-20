-- HMDA lending by zip and year, from the Spark job (jobs/hmda_lar.py).
select
    zip,
    cast(activity_year as integer) as activity_year,
    cast(purchase_loans_est as double) as purchase_loans_est,
    cast(investor_purchase_loans_est as double) as investor_purchase_loans_est,
    cast(second_home_purchase_loans_est as double) as second_home_purchase_loans_est,
    cast(investor_purchase_share as double) as investor_purchase_share,
    cast(median_loan_amount_est as double) as median_loan_amount_est,
    cast(tracts as integer) as tracts,
    xwalk_vintage,
    run_id
from {{ source('silver_spark', 'zip_lending_activity') }}
