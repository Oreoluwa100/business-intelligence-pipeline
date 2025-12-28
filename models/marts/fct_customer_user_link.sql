{{
    config(
        schema='marts',
        materialized='table',
        tags=['marts', 'bridge', 'fact', 'customers', 'users']
    )
}}

select
    link_id,
    customer_id,
    user_id,
    link_type,
    confidence_score,
    confidence_level,
    link_validation_status,
    created_date
from {{ ref('stg_customer_user_link') }}
where link_validation_status = 'Valid Link'