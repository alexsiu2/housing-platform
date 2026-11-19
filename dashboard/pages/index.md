---
sidebar_position: 1
title: National Overview
---

```sql info
select * from rpof.build_info
```

```sql headline
select
    count(*) as zips_scored,
    count(*) filter (where tier = 'strong') as strong_zips,
    round(median(gross_rental_yield_pct), 2) as median_yield,
    count(*) filter (where partial_score) as partial_zips
from rpof.scores
```

Explainable investment scores for US zip codes, built only from free public data (Zillow, Census ACS, BLS, HUD, FRED). Snapshot **<Value data={info} column=snapshot_month fmt="mmm yyyy" />**, scoring weights **<Value data={info} column=weights_version />**.

<BigValue data={headline} value=zips_scored title="Zips scored" fmt=num0 />
<BigValue data={headline} value=strong_zips title="Strong tier (score ≥ 70)" fmt=num0 />
<BigValue data={headline} value=median_yield title="Median gross yield (%)" fmt=num1 />
<BigValue data={headline} value=partial_zips title="Partial (no metro job data)" fmt=num0 />

## Top 20 opportunities

Each sub-score is a 0–100 percentile ("beats X% of zips"). Pick a zip on **[Zip Deep-Dive](/zip-deep-dive)** to see where its points come from.

```sql top20
select
    national_rank, zip, city, state, opportunity_score, tier,
    score_rental_yield, score_rent_growth, score_affordability,
    score_entry_price, score_demand_growth,
    gross_rental_yield_pct, rent_growth_1yr_pct, home_value, rent_index,
    partial_score
from rpof.scores
order by national_rank
limit 20
```

<DataTable data={top20} rows=20>
  <Column id=national_rank title="#" />
  <Column id=zip />
  <Column id=city />
  <Column id=state />
  <Column id=opportunity_score title="Score" fmt=num1 contentType=colorscale scaleColor=green />
  <Column id=score_rental_yield title="Yield" fmt=num0 />
  <Column id=score_rent_growth title="Rent growth" fmt=num0 />
  <Column id=score_affordability title="Afford." fmt=num0 />
  <Column id=score_entry_price title="Entry price" fmt=num0 />
  <Column id=score_demand_growth title="Jobs" fmt=num0 />
  <Column id=gross_rental_yield_pct title="Gross yield %" fmt=num1 />
  <Column id=home_value title="Home value" fmt=usd0 />
  <Column id=rent_index title="Rent" fmt=usd0 />
  <Column id=partial_score title="Partial" />
</DataTable>

## Where the strong zips are

```sql by_state
select
    state,
    count(*) as scored_zips,
    count(*) filter (where tier = 'strong') as strong_zips,
    round(100.0 * count(*) filter (where tier = 'strong') / count(*), 1) as strong_pct,
    round(median(opportunity_score), 1) as median_score
from rpof.scores
group by state
```

<USMap data={by_state} state=state abbreviations=true value=strong_zips title="Strong-tier zips by state" legend=true />

```sql distribution
select opportunity_score, tier from rpof.scores
```

<Histogram data={distribution} x=opportunity_score title="Score distribution" xAxisTitle="Opportunity score" />

## Macro backdrop

```sql macro
select month, mortgage_rate_30yr, cpi_shelter_yoy_pct, unemployment_rate
from rpof.macro
order by month
```

<LineChart data={macro} x=month y=mortgage_rate_30yr title="30-year mortgage rate (%)" yFmt=num1 />
<LineChart data={macro} x=month y=cpi_shelter_yoy_pct title="CPI shelter, year over year (%)" yFmt=num1 />

## How the score works

```sql weights
select
    case sub_score
        when 'score_rental_yield' then 'Rental yield'
        when 'score_rent_growth' then 'Rent growth'
        when 'score_affordability' then 'Affordability'
        when 'score_entry_price' then 'Entry price'
        when 'score_demand_growth' then 'Demand (jobs)'
    end as factor,
    metric, direction, weight,
    case when required then 'required'
         when missing = 'neutral' then 'optional: missing = 50'
         else 'optional: weight redistributed' end as if_missing
from rpof.weights
order by weight desc
```

<DataTable data={weights}>
  <Column id=factor />
  <Column id=metric title="Raw metric" />
  <Column id=direction title="Better when" />
  <Column id=weight fmt=pct0 />
  <Column id=if_missing title="If missing" />
</DataTable>

A zip is scored only when it has rental yield, rent growth and affordability, and a population of at least 1,000. Tiers: **strong** ≥ 70, **moderate** ≥ 45, otherwise **weak**. Gross yield ignores vacancy, repairs and taxes, so very high yields usually mean very cheap housing; check the deep-dive before trusting a number.
