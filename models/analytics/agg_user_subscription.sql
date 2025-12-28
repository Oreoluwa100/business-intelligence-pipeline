{{
    config(
        schema='analytics',
        materialized='table',
        tags=['analytics', 'saas_platform', 'users', 'subscriptions']
    )
}}

with users as (
    select * from {{ ref('dim_users') }}
),

subscriptions as (
    select 
        user_id, 
        plan_type, 
        start_date,
        row_number() over(partition by user_id order by start_date desc) as rn 
    from {{ ref('fct_subscriptions') }}
),

latest_subscriptions as (
    select * from subscriptions
    where rn = 1
)

select 
    u.user_id,
    u.signup_date,
    u.initial_plan,
    ls.start_date as latest_subscription_date,
    ls.plan_type as latest_plan
from users u join latest_subscriptions ls
on u.user_id = ls.user_id
