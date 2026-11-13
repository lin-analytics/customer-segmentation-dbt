-- One row per order line per snapshot; source is synthetic dbt seed.
select
    cast(snapshot_date as date) as snapshot_date,
    cast(order_id as string) as order_id,
    cast(order_line_id as string) as order_line_id,
    cast(customer_id as string) as customer_id,
    cast(product_id as string) as product_id,
    cast(bu as string) as bu,
    cast(order_date as date) as order_date,
    cast(requested_delivery_date as date) as requested_delivery_date,
    cast(open_quantity as int64) as open_quantity,
    cast(unit_price as numeric) as unit_price,
    cast(open_order_value as numeric) as open_order_value,
    cast(order_status as string) as order_status
from {{ ref('open_orders') }}
