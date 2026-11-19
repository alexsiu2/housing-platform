---
sidebar_position: 2
title: Zip Deep-Dive
---

```sql zip_options
select
    zip,
    zip || ' · ' || city || ', ' || state || '  (#' || cast(national_rank as integer) || ')' as label,
    national_rank
from rpof.scores
order by national_rank
```

<Dropdown data={zip_options} name=zip value=zip label=label title="Zip code" defaultValue="70601" order="national_rank" />

```sql z
select * from rpof.scores where zip = '${inputs.zip.value}'
```

```sql total
select count(*) as n from rpof.scores
```

{#if z.length > 0}

# {z[0].zip} · {z[0].city}, {z[0].state}

{z[0].metro ?? 'Not in a metro area'} · rank **#{z[0].national_rank}** of <Value data={total} column=n fmt=num0 /> scored zips{#if z[0].metro_rank} · **#{z[0].metro_rank}** of {z[0].metro_zip_count} in its metro{/if}

<BigValue data={z} value=opportunity_score title="Opportunity score" fmt=num1 />
<BigValue data={z} value=tier title="Tier" />
<BigValue data={z} value=home_value title="Typical home value" fmt=usd0 />
<BigValue data={z} value=rent_index title="Typical rent / month" fmt=usd0 />

{#if z[0].partial_score}
<Alert status=warning>
This zip has no recent metro job data (it is outside a BLS metro area), so its demand sub-score is set to the neutral 50.
</Alert>
{/if}

## Where the score comes from

Each bar is the sub-score's **points**: its 0–100 percentile times its weight. The points add up to the opportunity score.

```sql decomposition
with subs as (
    unpivot (
        select score_rental_yield, score_rent_growth, score_affordability,
               score_entry_price, score_demand_growth
        from rpof.scores where zip = '${inputs.zip.value}'
    ) on columns(*) into name sub_score value percentile
),
raw as (
    unpivot (
        select gross_rental_yield_pct::double as gross_rental_yield_pct,
               rent_growth_1yr_pct::double as rent_growth_1yr_pct,
               rent_to_income_ratio::double as rent_to_income_ratio,
               price_to_income_ratio::double as price_to_income_ratio,
               yoy_job_growth_pct::double as yoy_job_growth_pct
        from rpof.scores where zip = '${inputs.zip.value}'
    ) on columns(*) into name metric value raw_value
)
select
    case w.sub_score
        when 'score_rental_yield' then 'Rental yield'
        when 'score_rent_growth' then 'Rent growth'
        when 'score_affordability' then 'Affordability'
        when 'score_entry_price' then 'Entry price'
        when 'score_demand_growth' then 'Demand (jobs)'
    end as factor,
    w.metric,
    raw.raw_value,
    w.direction,
    subs.percentile,
    w.weight,
    round(subs.percentile * w.weight, 1) as points
from rpof.weights as w
join subs on subs.sub_score = w.sub_score
left join raw on raw.metric = w.metric
order by w.weight desc
```

<BarChart data={decomposition} x=factor y=points swapXY=true title="Points by sub-score" yAxisTitle="Points (of the 0–100 score)" sort=false />

<DataTable data={decomposition}>
  <Column id=factor />
  <Column id=metric title="Raw metric" />
  <Column id=raw_value title="Value" fmt=num2 />
  <Column id=direction title="Better when" />
  <Column id=percentile title="Percentile (0–100)" fmt=num1 />
  <Column id=weight fmt=pct0 />
  <Column id=points fmt=num1 totalAgg=sum />
</DataTable>

Raw metrics: gross yield and growth in %; rent-to-income and price-to-income as ratios (0.25 = 25% of local median income; 3.0 = three years of income). Census figures are for the ZCTA, which approximates the zip.

## History

```sql history
select month, rent_index, home_value, gross_rental_yield_pct, rent_growth_1yr_pct, home_value_growth_1yr_pct
from rpof.zip_history
where zip = '${inputs.zip.value}'
order by month
```

<LineChart data={history} x=month y=rent_index title="Typical rent ($/month)" yFmt=usd0 emptySet=pass emptyMessage="No rent history for this zip." />
<LineChart data={history} x=month y=home_value title="Typical home value ($)" yFmt=usd0 emptySet=pass emptyMessage="No value history for this zip." />
<LineChart data={history} x=month y={['rent_growth_1yr_pct', 'home_value_growth_1yr_pct']} title="Growth vs 12 months earlier (%)" yFmt=num1 emptySet=pass emptyMessage="No growth history for this zip." />

{:else}

Pick a zip above.

{/if}
