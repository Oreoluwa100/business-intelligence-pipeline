{{
    config(
        schema='marts',
        materialized='incremental',
        unique_key='event_id',
        tags=['marts', 'saas_platform', 'fact', 'incremental', 'events', 'users']
    )
}}

with events as (
    select * from {{ ref('stg_events') }}
),

users as (
    select * from {{ ref('dim_users') }}
)

select
    e.event_id,
    e.user_id,
    e.event_type,
    e.event_category,
    e.event_timestamp,
    e.event_date,
    e.event_hour,
    e.event_day_of_week,
    e.platform,
    e.session_id,
    e.view_duration_seconds,
    e.dashboard_id,
    e.report_type,
    e.chart_type,
    
    u.company_name,
    u.company_size_category,
    u.industry,
    
    case 
        when e.event_hour between 9 and 17 then 'Business Hours'
        when e.event_hour between 18 and 23 then 'Evening'
        else 'Night/Early Morning' 
    end as time_of_day_segment

from events e
left join users u on e.user_id = u.user_id

{% if is_incremental() %}
    where e.event_date > (select max(event_date) from {{ this }})
{% endif %}