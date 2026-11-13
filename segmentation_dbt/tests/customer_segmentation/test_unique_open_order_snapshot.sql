select snapshot_date, order_line_id
from {{ ref('fct_open_orders__line_snapshot') }}
group by 1,2
having count(*) > 1
