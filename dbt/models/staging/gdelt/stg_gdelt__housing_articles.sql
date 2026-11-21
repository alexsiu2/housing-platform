-- GDELT US housing articles, minus excluded sources (seeds/gdelt_excluded_sources).
-- Grain: article_key (GDELT record id + url).
select
    -- GDELT occasionally reuses a record id for two articles; (id, url) is unique.
    md5(a.article_id || '|' || a.url) as article_key,
    a.article_id,
    cast(a.published_at as timestamp) as published_at,
    cast(a.published_date as date) as published_date,
    a.url,
    a.source,
    cast(a.tone as double) as tone,
    a.housing_themes,
    a.zctas,
    a.places,
    a.run_id
from {{ source('bronze', 'gdelt_housing_articles_raw') }} as a
left join {{ ref('gdelt_excluded_sources') }} as x on x.source = a.source
where x.source is null
