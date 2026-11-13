select snapshot_date, opportunity_id
from {{ ref('fct_pipeline__opportunity_snapshot') }}
group by 1,2
having count(*) > 1
