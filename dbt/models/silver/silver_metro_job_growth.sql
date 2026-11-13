{#-
    Metro (CBSA) total nonfarm employment by month with year-over-year growth.

    Growth compares NSA levels against exactly 12 months earlier (standard for
    NSA series). If BLS ever returns a metro under two states, the series in
    the state with the most data wins, deterministically.
-#}
with ces as (
    select * from {{ ref('stg_bls__ces') }}
),

one_series_per_metro as (
    select cbsa_code, series_id
    from (
        select
            cbsa_code,
            series_id,
            row_number() over (
                partition by cbsa_code order by count(*) desc, series_id
            ) as rn
        from ces
        group by cbsa_code, series_id
    )
    where rn = 1
),

metro as (
    select ces.*
    from ces
    inner join one_series_per_metro using (cbsa_code, series_id)
)

select
    cur.cbsa_code,
    cur.month,
    cur.employment_thousands,
    prior.employment_thousands as employment_12mo_ago,
    round((cur.employment_thousands / nullif(prior.employment_thousands, 0) - 1) * 100, 2)
        as yoy_job_growth_pct,
    cur.is_preliminary,
    cur.series_id
from metro as cur
left join metro as prior
    on prior.cbsa_code = cur.cbsa_code
    and prior.month = cast(cur.month - interval 12 month as date)
