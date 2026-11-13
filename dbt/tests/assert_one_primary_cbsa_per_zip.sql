-- Every zip in the crosswalk has exactly one primary CBSA.
select zip, count(*) filter (where is_primary) as primaries
from {{ ref('silver_zip_cbsa_xwalk') }}
group by zip
having count(*) filter (where is_primary) <> 1
