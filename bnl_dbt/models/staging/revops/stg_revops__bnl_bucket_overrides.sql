select
    cast(as_of_month as date) as as_of_month,
    cast(bu as varchar) as bu,
    cast(analysis_customer_id as varchar) as analysis_customer_id,
    cast(product_id as varchar) as product_id,
    cast(override_bucket as varchar) as override_bucket,
    cast(reason as varchar) as reason
from {{ ref('bnl_bucket_overrides') }}