{#-
    Census tract -> zip allocation from the latest HUD vintage. res_ratio is
    the share of a tract's residential addresses in each zip, used to allocate
    tract-level HMDA metrics to zips (Phase 5).
-#}
with latest as (
    select max(vintage) as vintage
    from {{ ref('stg_hud__crosswalk') }}
    where crosswalk_type = 'tract-zip'
)

select
    x.geoid as tract,
    x.zip,
    x.res_ratio,
    x.bus_ratio,
    x.oth_ratio,
    x.tot_ratio,
    x.vintage
from {{ ref('stg_hud__crosswalk') }} as x
inner join latest using (vintage)
where x.crosswalk_type = 'tract-zip'
