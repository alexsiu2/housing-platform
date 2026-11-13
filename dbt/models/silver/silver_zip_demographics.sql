{#-
    ACS 5-year demographics by zip and vintage (ZCTA used as zip: a documented
    approximation). One row per (zip, vintage_year); suppressed estimates are
    null. renter_share = renter-occupied / occupied units.
-#}
with acs as (
    select * from {{ ref('stg_census__acs') }}
),

pivoted as (
    select
        zcta as zip,
        vintage_year,
        max(case when variable_name = 'median_household_income' then value end)
            as median_household_income,
        max(case when variable_name = 'population' then value end) as population,
        max(case when variable_name = 'occupied_units' then value end)
            as occupied_units,
        max(case when variable_name = 'renter_occupied_units' then value end)
            as renter_occupied_units,
        max(case when variable_name = 'unemployed' then value end) as unemployed,
        max(case when variable_name = 'median_gross_rent' then value end)
            as median_gross_rent
    from acs
    group by 1, 2
)

select
    *,
    round(renter_occupied_units / nullif(occupied_units, 0), 4) as renter_share
from pivoted
