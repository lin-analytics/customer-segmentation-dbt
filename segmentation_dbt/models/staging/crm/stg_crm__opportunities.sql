-- One row per opportunity per snapshot; source is synthetic dbt seed.
select
    cast(snapshot_date as date) as snapshot_date,
    cast(opportunity_id as string) as opportunity_id,
    cast(customer_id as string) as customer_id,
    cast(product_id as string) as product_id,
    cast(bu as string) as bu,
    cast(created_date as date) as created_date,
    cast(expected_close_date as date) as expected_close_date,
    cast(stage as string) as stage,
    cast(amount as numeric) as amount,
    cast(probability as numeric) as probability,
    cast(opportunity_name as string) as opportunity_name
from {{ ref('opportunities') }}
