---
sidebar_position: 5
title: Market Pulse
---

A weekly read of the housing news for the largest metros and the US as a whole, written by Claude (model and prompt version are listed under each outlook). Each outlook covers one Monday–Sunday week of streamed news (GDELT, plus five housing-news RSS feeds), and every sentence cites the news items it is based on, numbered as in the source list below. Metros where fewer than 2 of the week's items are really about housing get no outlook (location tags from GDELT also catch off-topic stories).

This is a summary of what the news says, not a forecast or investment advice. Check the sources.

```sql weeks
select distinct week_start, left(cast(week_start as varchar), 10) as label
from rpof.news_outlook
where week_start is not null
order by week_start desc
```

{#if weeks.length === 0}
<Alert status=info>
No outlooks yet. They are written every Monday by the <code>llm_metro_outlook</code> Airflow DAG.
</Alert>
{:else}

<Dropdown data={weeks} name=week value=label title="Week of" selectFirst=true order="label desc" />

```sql week_outlooks
select
    cbsa_code,
    metro_name,
    outlook,
    confidence,
    item_count,
    gdelt_avg_tone,
    key_themes as themes,
    (cbsa_code = 'national') as is_national
from rpof.news_outlook
where week_start = '${inputs.week.value}'
order by is_national desc, item_count desc
```

```sql counts
select
    count(*) filter (where not is_national) as metros,
    count(*) filter (where outlook = 'bullish' and not is_national) as bullish,
    count(*) filter (where outlook = 'neutral' and not is_national) as neutral,
    count(*) filter (where outlook = 'bearish' and not is_national) as bearish
from ${week_outlooks}
```

<BigValue data={counts} value=metros title="Metros with an outlook" fmt=num0 />
<BigValue data={counts} value=bullish title="Bullish" fmt=num0 />
<BigValue data={counts} value=neutral title="Neutral" fmt=num0 />
<BigValue data={counts} value=bearish title="Bearish" fmt=num0 />

<DataTable data={week_outlooks} rows=25 emptySet=pass emptyMessage="No outlooks for this week.">
  <Column id=metro_name title="Metro" />
  <Column id=outlook />
  <Column id=confidence />
  <Column id=item_count title="News items" fmt=num0 />
  <Column id=gdelt_avg_tone title="GDELT tone" fmt=num2 contentType=colorscale scaleColor=blue />
  <Column id=themes title="Key themes" />
</DataTable>

## One metro's outlook

```sql metro_options
select cbsa_code, metro_name || ' (' || outlook || ')' as label, is_national, item_count
from ${week_outlooks}
order by is_national desc, item_count desc
```

<Dropdown data={metro_options} name=metro value=cbsa_code label=label title="Metro" selectFirst=true order="item_count desc" />

```sql detail
select metro_name, outlook, confidence, summary, item_count,
       key_themes as themes,
       coalesce(nullif(risks, ''), 'none named') as risks,
       model_id, prompt_version, left(cast(generated_at as varchar), 10) as generated
from rpof.news_outlook
where week_start = '${inputs.week.value}' and cbsa_code = '${inputs.metro.value}'
```

{#if detail.length > 0}

### {detail[0].metro_name}: {detail[0].outlook} ({detail[0].confidence} confidence)

{detail[0].summary}

**Themes:** {detail[0].themes}
**Risks:** {detail[0].risks}

<small>From {detail[0].item_count} news items. Written by {detail[0].model_id} (prompt {detail[0].prompt_version}) on {detail[0].generated}.</small>

```sql sources
select ref_number, outlet, title, url, left(cast(published_at as varchar), 10) as published
from rpof.news_outlook_citations
where week_start = '${inputs.week.value}' and cbsa_code = '${inputs.metro.value}'
order by ref_number
```

<DataTable data={sources} rows=20 emptySet=pass emptyMessage="No cited sources.">
  <Column id=ref_number title="#" />
  <Column id=url title="Headline" contentType=link linkLabel=title openInNewTab=true />
  <Column id=outlet title="Source" />
  <Column id=published />
</DataTable>

{/if}

{/if}
