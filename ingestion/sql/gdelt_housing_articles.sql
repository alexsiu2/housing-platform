-- US housing news articles from GDELT's GKG, one row per article, run in
-- BigQuery where the 22 TB table lives. Only this small result leaves Google.
--
-- Cost control: only the day partitions in [@start_date, @end_date) and a few
-- compact v1 columns are read (~7.5 GB per month of news), plus the Census
-- ZCTA polygons (~1.2 GB). The client dry-runs this and refuses to exceed its
-- budget; every job also carries maximum_bytes_billed.
--
-- Location format (v1 Locations, ';'-separated):
--   Type#FullName#CountryCode#ADM1Code#Lat#Long#FeatureID   (Type 3 = US city)
-- A US city is one point (its centroid), placed in a ZCTA polygon. The zip is
-- only a route to the *metro* (mapped in dbt with HUD's current CBSA codes);
-- it is not zip-level news.
with housing_articles as (
    select
        GKGRECORDID as article_id,
        DATE as gdelt_timestamp,
        DocumentIdentifier as url,
        SourceCommonName as source,
        safe_cast(split(V2Tone, ',')[safe_offset(0)] as float64) as tone,
        Locations,
        array(
            select t from unnest(split(Themes, ';')) as t
            where t in unnest(@housing_themes)
        ) as housing_themes
    from `gdelt-bq.gdeltv2.gkg_partitioned`
    where _PARTITIONTIME >= timestamp(@start_date)
      and _PARTITIONTIME < timestamp(@end_date)
      and Locations like '%#US#%'
      and exists (
          select 1 from unnest(split(Themes, ';')) as t where t in unnest(@housing_themes)
      )
),

us_city_points as (
    select
        a.article_id,
        st_geogpoint(
            safe_cast(split(loc, '#')[safe_offset(5)] as float64),
            safe_cast(split(loc, '#')[safe_offset(4)] as float64)
        ) as point,
        split(loc, '#')[safe_offset(1)] as place
    from housing_articles as a, unnest(split(a.Locations, ';')) as loc
    where split(loc, '#')[safe_offset(0)] = '3'
      and split(loc, '#')[safe_offset(2)] = 'US'
),

article_zips as (
    select
        p.article_id,
        array_agg(distinct z.zip_code order by z.zip_code) as zctas,
        array_agg(distinct p.place order by p.place) as places
    from us_city_points as p
    join `bigquery-public-data.geo_us_boundaries.zip_codes` as z
        on st_contains(z.zip_code_geom, p.point)
    where p.point is not null
    group by p.article_id
)

select
    a.article_id,
    a.gdelt_timestamp,
    a.url,
    a.source,
    a.tone,
    a.housing_themes,
    z.zctas,
    z.places
from housing_articles as a
join article_zips as z using (article_id)
