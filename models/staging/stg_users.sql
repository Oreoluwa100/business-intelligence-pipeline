{{
    config(
        schema='staging',
        materialized='view',
        tags=['staging', 'saas_platform', 'users']
    )
}}

{{
    config(
        materialized='view',
        tags=['staging', 'saas']
    )
}}

select
    user_id,
    email,
    company_name,
    signup_date::date as signup_date,
    initial_plan,
    country,
    industry,
    employee_count,
    case employee_count
        when '1-10' then 'Small Business'
        when '11-50' then 'Medium Business'
        when '51-200' then 'Large Business'
        when '201-500' then 'Enterprise'
        when '500+' then 'Large Enterprise'
        else 'Unknown'
    end as company_size_category,
    current_date - signup_date::date as days_since_signup
from {{ source('saas_platform', 'users') }}