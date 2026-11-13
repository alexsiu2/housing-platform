{#-
    One row per zip per month where Zillow has both a rent and a home value:
    the raw inputs to the opportunity score (design doc §7.1).

      gross_rental_yield_pct = 12 x rent / home value x 100
      price_to_rent_ratio    = home value / (12 x rent)
      rent_to_income_ratio   = 12 x rent / median household income

    Point-in-time joins, so history stays honest:
      - ACS: newest vintage published by that month. A 5-year vintage V is
        released in December of V+1, so use vintage <= year - 2.
      - Jobs: latest metro month at or before the zip month (ASOF join);
        job_growth_month records which month it came from.
-#}
with rent as (
    select zip, month, rent_index, rent_growth_1yr_pct
    from {{ ref('silver_rent_index') }}
),

value as (
    select
        zip, month, home_value, home_value_growth_1yr_pct, cbsa_code,
        state, city, metro, county_name
    from {{ ref('silver_home_values') }}
),

base as (
    select value.*, rent.rent_index, rent.rent_growth_1yr_pct
    from value
    inner join rent using (zip, month)
),

with_demographics as (
    select
        base.*,
        d.median_household_income,
        d.population,
        d.renter_share,
        d.vintage_year as demographics_vintage
    from base
    left join {{ ref('silver_zip_demographics') }} as d
        on d.zip = base.zip
        and d.vintage_year = (
            select max(d2.vintage_year)
            from {{ ref('silver_zip_demographics') }} as d2
            where d2.zip = base.zip
              and d2.vintage_year <= extract(year from base.month) - 2
        )
),

jobs as (
    select cbsa_code, month as job_growth_month, yoy_job_growth_pct
    from {{ ref('silver_metro_job_growth') }}
    where yoy_job_growth_pct is not null
)

select
    w.zip,
    w.month,
    w.cbsa_code,
    w.metro,
    w.state,
    w.city,
    w.county_name,
    w.rent_index,
    w.home_value,
    w.rent_growth_1yr_pct,
    w.home_value_growth_1yr_pct,
    round(12 * w.rent_index / nullif(w.home_value, 0) * 100, 3)
        as gross_rental_yield_pct,
    round(w.home_value / nullif(12 * w.rent_index, 0), 3) as price_to_rent_ratio,
    round(12 * w.rent_index / nullif(w.median_household_income, 0), 4)
        as rent_to_income_ratio,
    w.median_household_income,
    w.population,
    w.renter_share,
    w.demographics_vintage,
    jobs.yoy_job_growth_pct,
    jobs.job_growth_month
from with_demographics as w
asof left join jobs
    on jobs.cbsa_code = w.cbsa_code
    and w.month >= jobs.job_growth_month
