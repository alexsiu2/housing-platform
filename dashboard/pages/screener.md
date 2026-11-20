---
sidebar_position: 4
title: Screener
---

Filter every scored zip; the table updates as you go and can be downloaded as CSV (hover over it).

```sql states
select distinct state from rpof.scores order by state
```

<Dropdown data={states} name=states value=state title="States" multiple=true selectAllByDefault=true />

<ButtonGroup name=tier title="Tier">
  <ButtonGroupItem valueLabel="All" value="%" default />
  <ButtonGroupItem valueLabel="Strong" value="strong" />
  <ButtonGroupItem valueLabel="Moderate" value="moderate" />
  <ButtonGroupItem valueLabel="Weak" value="weak" />
</ButtonGroup>

<Slider name=min_score title="Minimum score" min=0 max=100 step=5 defaultValue=0 />
<Slider name=min_yield title="Minimum gross yield (%)" min=0 max=20 step=0.5 defaultValue=0 />
<Slider name=max_price title="Maximum home value ($k; far right = no limit)" min=50 max=2500 step=50 defaultValue=2500 />

<ButtonGroup name=partial title="Metro job data">
  <ButtonGroupItem valueLabel="Include partial scores" value="include" default />
  <ButtonGroupItem valueLabel="Only zips with job data" value="exclude" />
</ButtonGroup>

```sql results
select
    national_rank, zip, city, state, metro, opportunity_score, tier,
    score_rental_yield, score_rent_growth, score_affordability,
    score_entry_price, score_demand_growth,
    gross_rental_yield_pct, rent_growth_1yr_pct, rent_to_income_ratio,
    price_to_income_ratio, yoy_job_growth_pct, investor_purchase_share,
    home_value, rent_index, partial_score
from rpof.scores
where state in ${inputs.states.value}
  and tier like '${inputs.tier}'
  and opportunity_score >= ${inputs.min_score}
  and gross_rental_yield_pct >= ${inputs.min_yield}
  and (${inputs.max_price} >= 2500 or home_value <= ${inputs.max_price} * 1000)
  and ('${inputs.partial}' = 'include' or not partial_score)
order by national_rank
```

```sql match_count
select count(*) as n from ${results}
```

**<Value data={match_count} column=n fmt=num0 /> zips match.**

<DataTable data={results} search=true rows=25 emptySet=pass emptyMessage="No zips match these filters.">
  <Column id=national_rank title="#" />
  <Column id=zip />
  <Column id=city />
  <Column id=state />
  <Column id=opportunity_score title="Score" fmt=num1 contentType=colorscale scaleColor=green />
  <Column id=tier />
  <Column id=gross_rental_yield_pct title="Yield %" fmt=num1 />
  <Column id=rent_growth_1yr_pct title="Rent growth %" fmt=num1 />
  <Column id=rent_to_income_ratio title="Rent / income" fmt=num2 />
  <Column id=price_to_income_ratio title="Price / income" fmt=num1 />
  <Column id=yoy_job_growth_pct title="Jobs %" fmt=num1 />
  <Column id=investor_purchase_share title="Investor share" fmt=pct1 />
  <Column id=home_value title="Home value" fmt=usd0 />
  <Column id=rent_index title="Rent" fmt=usd0 />
  <Column id=partial_score title="Partial" />
</DataTable>
