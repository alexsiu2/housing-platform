-- An HMDA year can only be used once published (from July 1 of the next year).
select zip, month, lending_data_vintage
from {{ ref('gold_zip_investment_metrics') }}
where lending_data_vintage is not null
  and make_date(lending_data_vintage + 1, 7, 1) > month
