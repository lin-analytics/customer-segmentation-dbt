-- One row per as_of_month x BU x product x resolved customer,
-- including reporting months with no sales (sales_amt = 0).
with reporting_spine as (
    select as_of_month, bu, product_id, analysis_customer_id
    from {{ ref('int_customer_segmentation__sales_windows') }}
),
monthly_sales as (
    select month, bu, product_id, analysis_customer_id, sales_amt
    from {{ ref('int_sales__product_customer_month') }}
),
bucket_final as (
    select as_of_month, bu, product_id, analysis_customer_id,
           logic_type, customer_segmentation_bucket_raw,
           customer_segmentation_bucket_final, override_applied_flag,
           override_reason, yoy_variance_amt
    from {{ ref('int_customer_segmentation__bucket_final') }}
)
select s.as_of_month, s.bu, s.product_id, s.analysis_customer_id,
       coalesce(m.sales_amt, 0) as sales_amt,
       b.logic_type, b.customer_segmentation_bucket_raw,
       b.customer_segmentation_bucket_final, b.override_applied_flag,
       b.override_reason, b.yoy_variance_amt
from reporting_spine s
left join monthly_sales m
  on m.month = s.as_of_month
 and m.bu = s.bu
 and m.product_id = s.product_id
 and m.analysis_customer_id = s.analysis_customer_id
left join bucket_final b
  on b.as_of_month = s.as_of_month
 and b.bu = s.bu
 and b.product_id = s.product_id
 and b.analysis_customer_id = s.analysis_customer_id
