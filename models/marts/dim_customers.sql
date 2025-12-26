{{
    config(
        schema='marts',
        materialized='table',
        tags=['marts', 'ecommerce', 'dimension']
    )
}}

select
    customer_id,
    email,
    first_name,
    last_name,
    signup_date,
    country_code,
    is_active_flag,
    region_group,
    data_quality_flag,
    days_since_signup,
    signup_month
from {{ ref('stg_customers') }}

