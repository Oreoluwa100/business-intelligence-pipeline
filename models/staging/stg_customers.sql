{{
    config(
        schema='staging',
        materialized='view',
        tags=['staging', 'ecommerce', 'customers']
    )
}}

with customers as (
    select
        customer_id,
        email,
        first_name,
        last_name,
        signup_date::date as signup_date,
        upper(country) as country_code,
        case 
            when is_active = 'True' then true
            when is_active = 'False' then false
            else null
        end as is_active_flag,
        case 
            when country in ('US', 'UK', 'Canada', 'Australia') then 'English-Speaking'
            when country = 'Germany' then 'DACH'
            else 'Other'
        end as region_group,
        case 
            when email not like '%@%.%' then 'Invalid Email Format'
            when signup_date > current_date then 'Future Signup Date'
            else 'Valid'
        end as data_quality_flag
    from {{ source('ecommerce', 'customers') }}
)

select 
    *,
    current_date - signup_date as days_since_signup,
    date_trunc('month', signup_date) as signup_month
from customers