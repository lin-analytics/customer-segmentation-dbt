select *
from {{ ref('int_customer_segmentation__bucket_final') }} bf
join {{ ref('stg_revops__customer_segmentation_bucket_overrides') }} o
  on bf.as_of_month = o.as_of_month
 and bf.bu = o.bu
 and bf.product_id = o.product_id
 and bf.analysis_customer_id = o.analysis_customer_id
where not (
    bf.override_applied_flag = 1
    and bf.customer_segmentation_bucket_final = o.override_bucket
)