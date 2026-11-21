-- US housing news per ISO week (every article once), for national context.
select
    cast(date_trunc('week', published_date) as date) as week_start,
    count(*) as article_count,
    round(avg(tone), 3) as avg_tone,
    count(*) filter (where tone < -2) as negative_articles,
    count(*) filter (where tone > 2) as positive_articles,
    count(distinct source) as sources
from {{ ref('stg_gdelt__housing_articles') }}
group by 1
