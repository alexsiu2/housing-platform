-- Evidence can't load an empty source (it writes a 0-byte Parquet file), so
-- until the first weekly outlook run this returns one all-null row. Pages
-- filter on week_start, which drops it.
select * from news_outlook_citations
union all by name
select null::date as week_start where not exists (select 1 from news_outlook_citations)
