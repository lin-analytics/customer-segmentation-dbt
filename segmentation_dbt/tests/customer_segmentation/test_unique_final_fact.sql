select as_of_month, bu, product_id, analysis_customer_id, count(*) as row_count
from {{ ref('fct_customer_segmentation__product_customer_month') }}
group by 1, 2, 3, 4
having count(*) != 1
