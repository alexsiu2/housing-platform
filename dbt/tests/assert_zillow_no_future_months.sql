-- Zillow months can never be later than the current month.
select 'rent' as dataset, zip, month from {{ ref('silver_rent_index') }}
where month > date_trunc('month', current_date)
union all
select 'value', zip, month from {{ ref('silver_home_values') }}
where month > date_trunc('month', current_date)
