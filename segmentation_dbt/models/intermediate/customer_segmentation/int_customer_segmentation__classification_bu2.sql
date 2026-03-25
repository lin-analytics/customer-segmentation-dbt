with w as (
    select * 
    from {{ ref('int_customer_segmentation__sales_windows') }}
),

final as (
    select
        as_of_month,
        bu,
        product_id,
        analysis_customer_id,
        sales_recent_9_months,
        sales_prior_9_27_months,
        yoy_variance_amt,
        case
            when sales_recent_9_months > 0 and sales_prior_9_27_months = 0 then 'New'
            when sales_recent_9_months = 0 and sales_prior_9_27_months > 0 then 'Inactive'
            when yoy_variance_amt > 0 then 'Growing'
            when yoy_variance_amt < 0 then 'Declining'
            else 'Stable'
        end as customer_segmentation_bucket_bu2
    from w  
)

select * from final