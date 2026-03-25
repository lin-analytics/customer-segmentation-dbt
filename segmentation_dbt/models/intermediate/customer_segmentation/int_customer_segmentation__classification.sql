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
        sales_recent_6_months,
        sales_prior_6_18_months,
        sales_ytd_current_year,
        sales_prior_year_full_year,
        sales_ytd_prior_year,
        yoy_variance_amt,
        
        case
            when month(as_of_month) <=6 then
                case
                    when sales_recent_6_months = 0 and sales_prior_6_18_months > 0 then 'Inactive'
                    when sales_recent_6_months > 0 and sales_prior_6_18_months = 0 then 'New'
                    when yoy_variance_amt > 0 then 'Growing'
                    when yoy_variance_amt < 0 then 'Declining'
                    else 'Stable'
                end
            else
                case
                    when sales_ytd_current_year = 0 and sales_ytd_prior_year > 0 then 'Inactive'
                    when sales_ytd_current_year > 0 and sales_ytd_prior_year = 0 then 'New'
                    when yoy_variance_amt > 0 then 'Growing'
                    when yoy_variance_amt < 0 then 'Declining'
                    else 'Stable'
                end
        end as customer_segmentation_bucket
    from w
),

final as (
    select 
        *
    from bucketed 
)

select * from final