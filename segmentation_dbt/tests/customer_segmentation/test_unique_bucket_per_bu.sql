select as_of_month, bu, product_id, analysis_customer_id, count(*) as row_count
from {{ ref('int_customer_segmentation__bucket_final') }}
group by 1, 2, 3, 4
having count(*) != 1
