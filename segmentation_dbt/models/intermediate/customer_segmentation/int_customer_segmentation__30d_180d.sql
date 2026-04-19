with w as (
    select *
    from {{ref('int_customer_segmentation__sales_windows')}}
),

bucketed as (
    select
        as_of_month,
        bu,
        product_id,
        analysis_customer_id,
        sales_recent_30d,
        sales_prior_180d,
        yoy_variance_amt,
        case
            when sales_recent_30d = 0 and sales_prior_180d > 0 then 'Inactive'
            when sales_recent_30d > 0 and sales_prior_180d = 0 then 'New'
            when yoy_variance_amt > 0 then 'Growing'
            when yoy_variance_amt < 0 then 'Declining'
            else 'Stable'
        end as customer_segmentation_bucket
    from w
),

final as (
    select 
        *
    from bucketed 
)

select * from final