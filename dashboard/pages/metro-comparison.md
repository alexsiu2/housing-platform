---
sidebar_position: 3
title: Metro Comparison
---

Metros (CBSAs) with at least 5 scored zips, compared on their zips' median score and the inputs behind it. Job growth is the metro's BLS total-nonfarm change vs 12 months earlier.

```sql metros
with news as (
    select cbsa_code, sum(article_count) as news_4wk,
           sum(avg_tone * article_count) / sum(article_count) as tone_4wk
    from rpof.metro_news
    where week_start >= (select max(week_start) from rpof.metro_news) - interval 21 day
    group by cbsa_code
)
select
    m.cbsa_code, m.metro, m.state, m.scored_zips, m.strong_zips, m.median_score,
    m.best_score, m.best_zip, m.median_yield_pct, m.median_rent_growth_pct,
    m.job_growth_pct, m.median_home_value, m.median_rent,
    news.news_4wk, round(news.tone_4wk, 2) as tone_4wk
from rpof.metro_summary as m
left join news using (cbsa_code)
where m.scored_zips >= 5
order by m.median_score desc
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
  <Column id=news_4wk title="News (4 wk)" fmt=num0 />
  <Column id=tone_4wk title="News tone" fmt=num2 contentType=colorscale scaleColor=blue />
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

## Housing news (GDELT)

Weekly US housing news mentioning places in this metro, from GDELT (filtered in BigQuery). Tone is GDELT's average article tone: below 0 is negative, above 0 positive; typical values sit between −3 and +3.

```sql news
select week_start, article_count, avg_tone, is_low_sample, is_national_policy_hub
from rpof.metro_news
where cbsa_code = '${inputs.metro.value}'
order by week_start
```

```sql news_flags
select bool_or(is_national_policy_hub) as policy_hub,
       avg(article_count) as avg_weekly_articles
from rpof.metro_news
where cbsa_code = '${inputs.metro.value}'
```

{#if news_flags[0]?.policy_hub}
<Alert status=info>
Washington, DC: GDELT tags federal housing-policy news with "Washington", so this mixes national policy news with local market news.
</Alert>
{/if}
{#if news_flags[0]?.avg_weekly_articles != null && news_flags[0].avg_weekly_articles < 10}
<Alert status=warning>
Thin coverage: about {Math.round(news_flags[0].avg_weekly_articles)} articles a week, so weekly tone here is mostly noise.
</Alert>
{/if}

<BarChart data={news} x=week_start y=article_count title="Housing articles per week" emptySet=pass emptyMessage="No GDELT housing news for this metro." />
<LineChart data={news} x=week_start y=avg_tone title="Average news tone" yFmt=num1 emptySet=pass emptyMessage="No GDELT housing news for this metro." />
