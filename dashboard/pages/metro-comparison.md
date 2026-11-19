---
sidebar_position: 3
title: Metro Comparison
---

Metros (CBSAs) with at least 5 scored zips, compared on their zips' median score and the inputs behind it. Job growth is the metro's BLS total-nonfarm change vs 12 months earlier.

```sql metros
select
    cbsa_code, metro, state, scored_zips, strong_zips, median_score, best_score,
    best_zip, median_yield_pct, median_rent_growth_pct, job_growth_pct,
    median_home_value, median_rent
from rpof.metro_summary
where scored_zips >= 5
order by median_score desc
```

<DataTable data={metros} search=true rows=15 emptySet=pass emptyMessage="No metro has 5+ scored zips in this snapshot.">
  <Column id=metro />
  <Column id=scored_zips title="Zips" />
  <Column id=strong_zips title="Strong" />
  <Column id=median_score title="Median score" fmt=num1 contentType=colorscale scaleColor=green />
  <Column id=best_zip title="Best zip" />
  <Column id=best_score title="Best score" fmt=num1 />
  <Column id=median_yield_pct title="Yield %" fmt=num1 />
  <Column id=median_rent_growth_pct title="Rent growth %" fmt=num1 />
  <Column id=job_growth_pct title="Jobs %" fmt=num1 />
  <Column id=median_home_value title="Home value" fmt=usd0 />
  <Column id=median_rent title="Rent" fmt=usd0 />
</DataTable>

<ScatterPlot
  data={metros}
  x=median_yield_pct
  y=median_rent_growth_pct
  series=state
  tooltipTitle=metro
  title="Yield vs rent growth (each dot is a metro)"
  xAxisTitle="Median gross yield (%)"
  yAxisTitle="Median rent growth (%)"
  legend=false
  emptySet=pass
  emptyMessage="No metros to plot."
/>

## Inside one metro

```sql metro_options
select cbsa_code, metro || ' (' || cast(scored_zips as integer) || ' zips)' as label, median_score
from rpof.metro_summary
where scored_zips >= 5
order by median_score desc
```

<Dropdown data={metro_options} name=metro value=cbsa_code label=label title="Metro" order="median_score desc" />

```sql metro_zips
select metro_rank, zip, city, opportunity_score, tier, national_rank,
       gross_rental_yield_pct, rent_growth_1yr_pct, home_value, rent_index
from rpof.scores
where cbsa_code = '${inputs.metro.value}'
order by metro_rank
```

<BarChart data={metro_zips} x=zip y=opportunity_score title="Zip scores in this metro" sort=false emptySet=pass emptyMessage="Pick a metro above." />

<DataTable data={metro_zips} rows=25 emptySet=pass emptyMessage="Pick a metro above.">
  <Column id=metro_rank title="Metro #" />
  <Column id=zip />
  <Column id=city />
  <Column id=opportunity_score title="Score" fmt=num1 contentType=colorscale scaleColor=green />
  <Column id=tier />
  <Column id=national_rank title="National #" />
  <Column id=gross_rental_yield_pct title="Yield %" fmt=num1 />
  <Column id=rent_growth_1yr_pct title="Rent growth %" fmt=num1 />
  <Column id=home_value title="Home value" fmt=usd0 />
  <Column id=rent_index title="Rent" fmt=usd0 />
</DataTable>
