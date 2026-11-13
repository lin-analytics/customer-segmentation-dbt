with observations as (
    select 'orders' as src, order_line_id as row_id, product_id, bu
    from {{ ref('fct_open_orders__line_snapshot') }}
    union all
    select 'pipeline' as src, opportunity_id as row_id, product_id, bu
    from {{ ref('fct_pipeline__opportunity_snapshot') }}
)
select o.*
from observations o
left join {{ ref('dim_product') }} p on o.product_id = p.product_id
where p.product_id is null or o.bu != p.bu
