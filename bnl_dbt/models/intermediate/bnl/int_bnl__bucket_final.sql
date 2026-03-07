with std as (
    select
        as_of_month,
        bu,
        product_id,
        analysis_customer_id,
        bnl_bucket as bnl_bucket_raw,
        yoy_variance_amt,
        'standard' as logic_type
    from {{ref('int_bnl__classification')}}
),

bu2 as (
    select
        as_of_month,
        bu,
        product_id,
        analysis_customer_id,
        bnl_bucket_bu2 as bnl_bucket_raw,
        cast(null as double) as yoy_variance_amt,
        'bu2_9_18' as logic_type
    from {{ref('int_bnl__classification_bu2')}}
),

raw as (
    select * from std
    union all
    select * from bu2
),

ovr as (
    select *
    from {{ref('stg_revops__bnl_bucket_overrides')}}
),

joined as (
    select
        r.*,    
        o.override_bucket,
        o.reason as override_reason
    from raw r
    left join ovr o
    on r.as_of_month = o.as_of_month
    and r.bu = o.bu
    and r.analysis_customer_id = o.analysis_customer_id
    and r.product_id = o.product_id
),

final as (
    select
        as_of_month,
        bu,
        product_id,
        analysis_customer_id,
        bnl_bucket_raw,
        case 
            when override_bucket is not null then override_bucket
            else bnl_bucket_raw
        end as bnl_bucket_final,
        case
            when override_bucket is not null then 1 else 0
        end as override_applied_flag,
        yoy_variance_amt,
        logic_type,
        override_reason
    from joined
)

select * from final