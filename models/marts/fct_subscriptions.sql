{{
    config(
        schema='marts',
        materialized='incremental',
        unique_key='subscription_id',
        tags=['marts', 'saas_platform', 'fact', 'incremental', 'subscriptions', 'users']
    )
}}

with subscriptions as (
    select * from {{ ref('stg_subscriptions') }}
),

users as (
    select * from {{ ref('dim_users') }}
)

select
    s.subscription_id,
    s.user_id,
    s.plan_type,
    s.start_date,
    s.end_date,
    s.price,
    s.status,
    s.is_currently_active,
    s.duration_days,
    s.monthly_recurring_revenue,
    s.plan_level,
    
    u.company_name,
    u.country,
    u.industry,
    u.company_size_category

from subscriptions s
left join users u on s.user_id = u.user_id

{% if is_incremental() %}
    where s.start_date > (select max(start_date) from {{ this }})
{% endif %}