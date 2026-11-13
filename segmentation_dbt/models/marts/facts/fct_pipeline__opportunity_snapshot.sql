-- Grain: snapshot_date x opportunity_id.
-- Weighted amount represents a *scenario estimate*, not booked or recognized revenue.
with source as (
    select * from {{ ref('stg_crm__opportunities') }}
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
    snapshot_date, opportunity_id, customer_id, analysis_customer_id,
    product_id, bu, created_date, expected_close_date, opportunity_name,
    stage, amount, probability,
    stage not in ('Closed Won','Closed Lost') as is_open,
    case when stage not in ('Closed Won','Closed Lost') then amount else 0 end as open_pipeline_amount,
    case when stage not in ('Closed Won','Closed Lost') then amount * probability else 0 end as weighted_open_pipeline_amount
from resolved
