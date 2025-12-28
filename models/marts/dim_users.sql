{{
    config(
        schema='marts',
        materialized='table',
        tags=['marts', 'saas_platform', 'dimension', 'users']
    )
}}

select
    user_id,
    email,
    company_name,
    signup_date,
    initial_plan,
    country,
    industry,
    employee_count,
    company_size_category,
    days_since_signup
from {{ ref('stg_users') }}