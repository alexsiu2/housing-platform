-- An ACS vintage can only be used once published (vintage V: December V+1).
select zip, month, demographics_vintage
from {{ ref('gold_zip_investment_metrics') }}
where demographics_vintage > extract(year from month) - 2
