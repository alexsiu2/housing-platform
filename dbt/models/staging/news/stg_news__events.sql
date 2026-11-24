-- Streamed news items (RSS, live GDELT), cleaned: one row per item_key.
-- GDELT items from excluded domains (seeds/gdelt_excluded_sources) are dropped,
-- as are items without a real title. All sources are English-language (GDELT's
-- English stream, English RSS feeds), so no language filter is needed yet.
with src as (
    select * from {{ source('bronze', 'news_events_raw') }}
),

canon as (
    select
        *,
        -- Canonical URL for cross-source dedupe: no scheme/www, fragment or
        -- tracking parameters, no trailing slash.
        rtrim(
            regexp_replace(
                regexp_replace(
                    regexp_replace(lower(url), '^https?://(www\.)?', ''),
                    '#.*$', ''
                ),
                '([?&])(utm_[a-z_]+|fbclid|gclid|cmpid|mc_cid|mc_eid)=[^&]*', '\1', 'g'
            ),
            '/?&'
        ) as canonical_url
    from src
)

select
    md5(c.source || '|' || c.source_id) as item_key,
    c.source,
    c.source_id,
    c.url,
    c.canonical_url,
    c.title,
    c.body_snippet,
    -- Items without a publish time (rare RSS) fall back to when we saw them.
    cast(coalesce(c.published_at, c.fetched_at) as timestamp) as published_at,
    cast(c.fetched_at as timestamp) as fetched_at,
    c.feed_name,
    c.subreddit,
    c.geo_hint,
    c.engagement,
    cast(c.tone as double) as tone,
    c.themes,
    c.run_id
from canon as c
left join {{ ref('gdelt_excluded_sources') }} as x
    on c.source = 'gdelt' and x.source = c.feed_name
where x.source is null
  and nullif(trim(c.title), '') is not null
  -- GDELT sometimes has only the site's name as the title ("example.com").
  and lower(trim(c.title)) <> lower(coalesce(c.feed_name, ''))
