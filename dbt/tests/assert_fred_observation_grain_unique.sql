-- Staging grain must be one row per (series_id, observation_date).
select series_id, observation_date, count(*) as n
from {{ ref('stg_fred__observations') }}
group by 1, 2
having count(*) > 1
