with spine as (
    select
        month as as_of_month,
        bu,
        product_id,
        analysis_customer_id,
        sales_amt
    from {{ ref('int_sales__product_customer_month') }}
),

bucket_final as (
    select
        as_of_month,
        bu,
        product_id,
        analysis_customer_id,
        logic_type,
        customer_segmentation_bucket_raw,
        customer_segmentation_bucket_final,
        override_applied_flag,
        override_reason,
        yoy_variance_amt
    from {{ ref('int_customer_segmentation__bucket_final') }}
),

final as (
    select
        s.as_of_month,
        s.bu,
        s.product_id,
        s.analysis_customer_id,
        s.sales_amt,

        b.logic_type,
        b.customer_segmentation_bucket_raw,
        b.customer_segmentation_bucket_final,
        b.override_applied_flag,
        b.override_reason,
        b.yoy_variance_amt

    from spine s
    left join bucket_final b
      on s.as_of_month = b.as_of_month
     and s.bu = b.bu
     and s.product_id = b.product_id
     and s.analysis_customer_id = b.analysis_customer_id
)

select * from final