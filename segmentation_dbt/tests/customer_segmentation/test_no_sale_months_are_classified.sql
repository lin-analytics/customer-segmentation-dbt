select *
from {{ ref('fct_customer_segmentation__product_customer_month') }}
where sales_amt = 0 and customer_segmentation_bucket_final is null
