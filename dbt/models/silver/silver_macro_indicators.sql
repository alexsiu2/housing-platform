{#-
    National macro indicators at monthly grain, one row per month.

    - Weekly series (mortgage rate) are averaged within the month.
    - Monthly series pass through.
    - Quarterly series (median sale price) and any gaps are forward-filled from
      the latest prior observation, so every month joins cleanly downstream.
      `*_observed_month` columns record which month each value actually came
      from, so staleness stays visible.
-#}

{%- set series = {
    'MORTGAGE30US': 'mortgage_rate_30yr',
    'CUUR0000SAH': 'cpi_shelter',
    'HOUST': 'housing_starts_k',
    'UNRATE': 'unemployment_rate',
    'CSUSHPISA': 'case_shiller_hpi',
    'MSPUS': 'median_sale_price',
} -%}

with monthly as (
    select
        series_id,
        cast(date_trunc('month', observation_date) as date) as month,
        avg(value) as value
    from {{ ref('stg_fred__observations') }}
    group by 1, 2
),

bounds as (
    select min(month) as first_month, max(month) as last_month from monthly
),

spine as (
    select
        cast(unnest(generate_series(first_month, last_month, interval 1 month)) as date)
            as month
    from bounds
),

pivoted as (
    select
        spine.month,
        {%- for series_id, col in series.items() %}
        max(case when m.series_id = '{{ series_id }}' then m.value end) as {{ col }}_raw,
        max(case when m.series_id = '{{ series_id }}' then m.month end)
            as {{ col }}_observed_month
        {%- if not loop.last %},{% endif %}
        {%- endfor %}
    from spine
    left join monthly as m on m.month = spine.month
    group by spine.month
),

filled as (
    select
        month,
        {%- for col in series.values() %}
        last_value({{ col }}_raw ignore nulls) over w as {{ col }},
        last_value({{ col }}_observed_month ignore nulls) over w
            as {{ col }}_observed_month
        {%- if not loop.last %},{% endif %}
        {%- endfor %}
    from pivoted
    window w as (order by month rows between unbounded preceding and current row)
)

select
    *,
    round(
        (cpi_shelter / nullif(lag(cpi_shelter, 12) over (order by month), 0) - 1) * 100,
        2
    ) as cpi_shelter_yoy_pct,
    round(
        (case_shiller_hpi / nullif(lag(case_shiller_hpi, 12) over (order by month), 0) - 1)
        * 100,
        2
    ) as case_shiller_hpi_yoy_pct
from filled
order by month
