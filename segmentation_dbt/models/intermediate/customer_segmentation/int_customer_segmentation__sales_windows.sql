with spine as (
    select
        month,
        bu,
        product_id,
        analysis_customer_id,
        sales_amt
    from {{ref('int_sales__product_customer_month')}}
),

as_of_months as (
    -- build an as-of frame from the months that exist in the data
    select distinct
        month as as_of_month
    from spine
),

joined as (
    -- for each as_of_month, bring in history rows up to that month
    select
        a.as_of_month,
        s.month,
        s.bu,
        s.product_id,
        s.analysis_customer_id,
        s.sales_amt  
    from as_of_months a
    left join spine s
        on s.month <= a.as_of_month
),

final as (
    select
        as_of_month,
        bu,
        product_id,
        analysis_customer_id,
        sales_recent_30d,
        sales_prior_180d,
        sales_ytd_current_year,
        sales_prior_year_full_year,
        sales_ytd_prior_year,
        sales_recent_90d,
        sales_prior_360d,
        (sales_ytd_current_year - sales_ytd_prior_year) as yoy_variance_amt
    from(
        select
            as_of_month,
            bu,
            product_id,
            analysis_customer_id,

    -- Recent 30 days not including as_of_month
    sum(
        case
            when month >= (as_of_month - interval '30 day') 
            and month < as_of_month then sales_amt
            else 0
        end
    ) as sales_recent_30d,

    -- Prior window: prior 30-180 days.
    sum(
        case
            when month >= (as_of_month - interval '180 day') 
            and month < (as_of_month - interval '30 day') then sales_amt
            else 0
        end
    ) as sales_prior_180d,

    -- YTD Current year sales (from Jan 1 of as_of_month's year to as_of_month, not including as_of_month the as_of_month)
    sum(
        case
            when year(month) = year(as_of_month)
            and month < as_of_month then sales_amt
            else 0
        end
    ) as sales_ytd_current_year,

    -- Prior year full year sales (from Jan 1 to Dec 31 of the year prior to as_of_month's year)
    sum(
        case
            when year(month) = (year(as_of_month) - 1)
            then sales_amt
            else 0
        end
    ) as sales_prior_year_full_year,

    -- Prior year to date sales
    sum(
        case
            when year(month) = year(as_of_month) -1
            and month < (as_of_month - interval '1 year') then sales_amt
            else 0
        end
    ) as sales_ytd_prior_year,

    sum(
        case
            when month >= (as_of_month - interval '90 day') 
            and month < as_of_month then sales_amt
            else 0
        end
    ) as sales_recent_90d,

     -- Prior window: prior 90-360 days.
    sum(
        case
            when month >= (as_of_month - interval '360 day') 
            and month < (as_of_month - interval '90 day') then sales_amt
            else 0
        end
    ) as sales_prior_360d
    from joined
    group by 1,2,3,4
) x
)

select * from final