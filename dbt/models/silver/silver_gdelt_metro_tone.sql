{#-
    US housing news per metro (CBSA) and ISO week from GDELT: article volume
    and average tone (design doc §6.3). Built from article-level rows so each
    article counts once per metro, with metros from HUD's *current* zip->CBSA
    codes (Census polygons in BigQuery use the pre-2023 delineation).

    Caveats, kept visible as columns:
      - is_low_sample: < 10 articles; weekly tone is noise below that.
      - is_national_policy_hub: GDELT tags federal housing-policy news with
        "Washington", so DC (47900) mixes national and local news.
-#}
with articles as (
    select article_key, published_date, tone, unnest(zctas) as zip
    from {{ ref('stg_gdelt__housing_articles') }}
),

article_metro as (
    select distinct
        a.article_key,
        cast(date_trunc('week', a.published_date) as date) as week_start,
        a.tone,
        x.cbsa_code
    from articles as a
    inner join {{ ref('silver_zip_cbsa_xwalk') }} as x
        on x.zip = a.zip and x.is_primary
)

select
    cbsa_code,
    week_start,
    count(*) as article_count,
    round(avg(tone), 3) as avg_tone,
    round(stddev_samp(tone), 3) as tone_stddev,
    count(*) filter (where tone < -2) as negative_articles,
    count(*) filter (where tone > 2) as positive_articles,
    count(*) < 10 as is_low_sample,
    cbsa_code = '47900' as is_national_policy_hub
from article_metro
group by cbsa_code, week_start
