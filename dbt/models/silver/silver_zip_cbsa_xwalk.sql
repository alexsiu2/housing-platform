{#-
    Zip -> metro (CBSA) mapping from the latest HUD vintage.

    A zip can straddle metros; every pair is kept with its residential share,
    and `is_primary` marks the CBSA holding most of the zip's residential
    addresses. HUD's 99999 ("not in a CBSA") rows are dropped, so rural zips
    simply have no row.
-#}
with latest as (
    select max(vintage) as vintage
    from {{ ref('stg_hud__crosswalk') }}
    where crosswalk_type = 'zip-cbsa'
)

select
    x.zip,
    x.geoid as cbsa_code,
    x.res_ratio,
    x.tot_ratio,
    row_number() over (
        partition by x.zip order by x.res_ratio desc, x.tot_ratio desc, x.geoid
    ) = 1 as is_primary,
    x.vintage
from {{ ref('stg_hud__crosswalk') }} as x
inner join latest using (vintage)
where x.crosswalk_type = 'zip-cbsa'
  and x.geoid <> '99999'
