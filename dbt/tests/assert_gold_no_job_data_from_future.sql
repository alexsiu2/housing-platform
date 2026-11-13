-- The ASOF join must never use a BLS month later than the Zillow month.
select zip, month, job_growth_month
from {{ ref('gold_zip_investment_metrics') }}
where job_growth_month > month
