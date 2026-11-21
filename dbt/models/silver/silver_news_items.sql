{#-
    News items tagged with metros, for the weekly LLM outlook (design §6.3, §9.3).
    Grain: item_key x metro_tag (an article about two metros appears twice).

    1. Dedupe across sources on canonical URL; keep the copy with a snippet.
    2. Tag, in priority order (the first method that finds any metro wins):
         a. Reddit city subreddit -> metro (config/subreddit_metro_map.yaml),
            once the Reddit producer exists.
         b. gdelt_geo: GDELT's US city locations (city + state code) matched
            to seeds/metro_aliases.
         c. keyword: "City, ST" or "City, State" in the title/snippet, or the
            bare city name where the alias is unambiguous (bare_ok).
         d. national: nothing matched.
-#}
with items as (
    select *
    from {{ ref('stg_news__events') }}
    qualify row_number() over (
        partition by canonical_url
        order by (body_snippet is not null) desc, published_at, item_key
    ) = 1
),

aliases as (
    select cbsa_code, city, state_abbr, state_name, bare_ok
    from {{ ref('metro_aliases') }}
),

gdelt_locations as (
    select
        i.item_key,
        split_part(split_part(loc, '|', 1), ',', 1) as city,
        right(split_part(loc, '|', 2), 2) as state_abbr
    from items as i,
        unnest(string_split(i.geo_hint, ';')) as g(loc)
    where i.geo_hint is not null
),

gdelt_geo as (
    select distinct l.item_key, a.cbsa_code
    from gdelt_locations as l
    inner join aliases as a
        on lower(a.city) = lower(l.city) and a.state_abbr = l.state_abbr
),

texts as (
    -- Names of institutions and papers that contain a city ("New York Fed",
    -- "Federal Reserve Bank of Dallas", "Los Angeles Times") are removed
    -- first: they are not news about that city.
    select
        item_key,
        regexp_replace(
            regexp_replace(
                title || ' ' || coalesce(body_snippet, ''),
                'Federal Reserve Bank of ([A-Z][a-z]+\.? ){0,2}', ' ', 'g'
            ),
            '([A-Z][a-z]+\.? ){1,2}(Fed|Times|Post|Globe|Tribune|Chronicle|Inquirer'
            || '|Herald|Journal|Business Journal)\b',
            ' ', 'g'
        ) as txt
    from items
),

keyword as (
    select distinct t.item_key, a.cbsa_code
    from texts as t
    -- contains() is a cheap prefilter before the regex.
    inner join aliases as a on contains(t.txt, a.city)
    where regexp_matches(
            t.txt,
            '\b' || regexp_escape(a.city) || ',? (' || a.state_abbr || '|'
            || regexp_escape(a.state_name) || ')\b'
        )
        or (a.bare_ok and regexp_matches(t.txt, '\b' || regexp_escape(a.city) || '\b'))
),

tags as (
    select item_key, cbsa_code, 'gdelt_geo' as metro_tag_method from gdelt_geo
    union all
    select k.item_key, k.cbsa_code, 'keyword'
    from keyword as k
    anti join gdelt_geo as g on g.item_key = k.item_key
)

select
    i.item_key,
    coalesce(t.cbsa_code, 'national') as metro_tag,
    t.cbsa_code,
    coalesce(t.metro_tag_method, 'national') as metro_tag_method,
    i.source,
    i.source_id,
    i.url,
    i.canonical_url,
    i.title,
    i.body_snippet,
    i.published_at,
    cast(i.published_at as date) as published_date,
    cast(date_trunc('week', i.published_at) as date) as week_start,
    i.feed_name,
    i.subreddit,
    i.engagement,
    i.tone,
    i.themes
from items as i
left join tags as t on t.item_key = i.item_key
