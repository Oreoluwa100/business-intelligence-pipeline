{{
    config(
        schema='staging',
        materialized='view',
        tags=['staging', 'saas_platform', 'subscriptions']
    )
}}

with subscriptions as (
    select
        subscription_id,
        user_id,
        plan_type,
        start_date::date as start_date,
        end_date::date as end_date,
        price,
        status,
        case 
            when status = 'active' 
            then true 
            else false 
        end as is_currently_active,
        case 
            when end_date is not null 
            then (end_date - start_date) 
            else null 
        end as duration_days
    from {{ source('saas_platform', 'subscriptions') }}
)

select *,
    case 
        when is_currently_active then price
        else 0
    end as monthly_recurring_revenue,
    case plan_type
        when 'free' then 1
        when 'basic' then 2
        when 'premium' then 3
        when 'enterprise' then 4
        else 0
    end as plan_level
from subscriptions