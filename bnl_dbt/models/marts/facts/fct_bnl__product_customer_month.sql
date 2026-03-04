with spine as (
    select *
    from {{ref('int_sales__product_customer_month')}}
),

bnl_std as (
    select
        as_of_month,
        bu,
        product_id,
        analysis_customer_id,
        bnl_bucket,
        yoy_variance_amt,
    from {{ref('int_bnl__classification')}}
),

bnl_bu2 as (
    select 
        as_of_month,
        bu,
        product_id,
        analysis_customer_id,
        bnl_bucket_bu2,
        yoy_variance_amt
    from {{ref('int_bnl__classification_bu2')}}
),

joined as (
    select
        s.month as as_of_month,
        s.bu,
        s.product_id,
        s.analysis_customer_id,
        s.sales_amt,
        std.bnl_bucket as bnl_bucket_std,
        std.yoy_variance_amt,
        bu2.bnl_bucket_bu2
    from spine s
    left join bnl_std std
     on s.month = std.as_of_month 
     and s.bu = std.bu 
     and s.product_id = std.product_id 
     and s.analysis_customer_id = std.analysis_customer_id
    left join bnl_bu2 bu2 
    on s.month = bu2.as_of_month 
    and s.bu = bu2.bu 
    and s.product_id = bu2.product_id 
    and s.analysis_customer_id = bu2.analysis_customer_id
),

final as (
    select
        as_of_month,
        bu,
        product_id,
        analysis_customer_id,
        sales_amt,
        case 
            when bu = 'BU_B' then coalesce(bnl_bucket_bu2, bnl_bucket_std)  
            else bnl_bucket_std
        end as bnl_bucket,
        yoy_variance_amt,

        bnl_bucket_std,
        bnl_bucket_bu2
    from joined
)

select * from final