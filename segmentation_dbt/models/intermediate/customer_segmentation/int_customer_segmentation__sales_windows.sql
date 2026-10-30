-- Report as of the FIRST day of a month, using only completed prior months.
-- Monthly equivalents of the original 30/180- and 90/360-day windows:
-- 1 recent + 5 earlier months, and 3 recent + 9 earlier months.
-- A reporting spine ensures no-sale customer/product months are retained.
with monthly_sales as (
    select month, bu, product_id, analysis_customer_id, sales_amt
    from {{ ref('int_sales__product_customer_month') }}
),
reporting_months as (
    select as_of_month
    from unnest(generate_date_array(
        (select min(month) from monthly_sales),
        date_add((select max(month) from monthly_sales), interval 1 month),
        interval 1 month
    )) as as_of_month
),
customer_products as (
    select bu, product_id, analysis_customer_id, min(month) as first_sales_month
    from monthly_sales
    group by 1, 2, 3
),
reporting_spine as (
    select m.as_of_month, c.bu, c.product_id, c.analysis_customer_id
    from reporting_months m
    cross join customer_products c
    where m.as_of_month >= c.first_sales_month
),
joined as (
    select s.as_of_month, s.bu, s.product_id, s.analysis_customer_id,
           t.month, t.sales_amt
    from reporting_spine s
    left join monthly_sales t
      on t.bu = s.bu
     and t.product_id = s.product_id
     and t.analysis_customer_id = s.analysis_customer_id
     and t.month < s.as_of_month
),
aggregated as (
    select as_of_month, bu, product_id, analysis_customer_id,
        sum(case when month >= date_sub(as_of_month, interval 1 month)
                  then coalesce(sales_amt, 0) else 0 end) as sales_recent_30d,
        sum(case when month >= date_sub(as_of_month, interval 6 month)
                   and month < date_sub(as_of_month, interval 1 month)
                  then coalesce(sales_amt, 0) else 0 end) as sales_prior_180d,
        sum(case when month >= date_sub(as_of_month, interval 3 month)
                  then coalesce(sales_amt, 0) else 0 end) as sales_recent_90d,
        sum(case when month >= date_sub(as_of_month, interval 12 month)
                   and month < date_sub(as_of_month, interval 3 month)
                  then coalesce(sales_amt, 0) else 0 end) as sales_prior_360d,
        sum(case when extract(year from month) = extract(year from as_of_month)
                   and month < as_of_month
                  then coalesce(sales_amt, 0) else 0 end) as sales_ytd_current_year,
        sum(case when extract(year from month) = extract(year from as_of_month) - 1
                  then coalesce(sales_amt, 0) else 0 end) as sales_prior_year_full_year,
        sum(case when extract(year from month) = extract(year from as_of_month) - 1
                   and extract(month from month) < extract(month from as_of_month)
                  then coalesce(sales_amt, 0) else 0 end) as sales_ytd_prior_year
    from joined
    group by 1, 2, 3, 4
)
select *, sales_ytd_current_year - sales_ytd_prior_year as yoy_variance_amt
from aggregated
