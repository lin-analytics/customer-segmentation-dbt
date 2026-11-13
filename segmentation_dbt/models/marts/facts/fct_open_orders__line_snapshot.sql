-- Grain: snapshot_date x order_line_id.
-- Resolve retired IDs using the exact same merge map as the sales models.
-- Keep this fact SEPARATE from sales and pipeline to avoid fanout.
with source as (
    select * from {{ ref('stg_erp__open_orders') }}
),
merge_map as (
    select old_customer_id, survivor_customer_id
    from {{ ref('stg_business__customer_merge_map') }}
),
resolved as (
    select
        s.*,
        coalesce(m.survivor_customer_id, s.customer_id) as analysis_customer_id
    from source s
    left join merge_map m on s.customer_id = m.old_customer_id
)
select
    snapshot_date, order_id, order_line_id, customer_id, analysis_customer_id,
    product_id, bu, order_date, requested_delivery_date, order_status,
    open_quantity, unit_price, open_order_value,
    requested_delivery_date < snapshot_date as is_overdue,
    date_diff(snapshot_date, requested_delivery_date, day) as days_past_due
from resolved
