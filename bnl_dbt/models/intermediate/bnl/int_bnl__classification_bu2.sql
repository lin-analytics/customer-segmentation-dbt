with w as (
    select * 
    from {{ ref('int_bnl__sales_windows') }}
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
            when sales_recent_9_months = 0 and sales_prior_9_27_months > 0 then 'Lost'
            else 'Base'
        end as bnl_bucket_bu2
    from w  
)

select * from final