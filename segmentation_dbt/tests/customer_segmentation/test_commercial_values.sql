with errors as (
    select 'orders' as src, order_line_id as row_id
    from {{ ref('fct_open_orders__line_snapshot') }}
    where open_quantity <= 0 or unit_price < 0 or open_order_value < 0
       or abs(open_order_value - open_quantity * unit_price) > 0.02
       or is_overdue != (requested_delivery_date < snapshot_date)
    union all
    select 'pipeline' as src, opportunity_id as row_id
    from {{ ref('fct_pipeline__opportunity_snapshot') }}
    where amount < 0 or probability < 0 or probability > 1
       or abs(weighted_open_pipeline_amount - (case when is_open then amount * probability else 0 end)) > 0.02
       or (not is_open and open_pipeline_amount != 0)
)
select * from errors
