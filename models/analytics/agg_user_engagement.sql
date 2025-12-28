{{
    config(
        schema='analytics',
        materialized='incremental',
        unique_key=['user_id', 'event_date'],
        tags=['analytics', 'saas_platform', 'incremental', 'users', 'events']
    )
}}

with daily_events as (
    select
        user_id,
        event_date,
        count(*) as total_events,
        count(distinct event_type) as unique_event_types,
        min(event_timestamp) as first_event_timestamp,
        max(event_timestamp) as last_event_timestamp
    from {{ ref('fct_events') }}
    
    {% if is_incremental() %}
        where event_date > (select max(event_date) from {{ this }})
    {% endif %}
    
    group by user_id, event_date
)

select
    user_id,
    event_date,
    total_events,
    unique_event_types,
    first_event_timestamp,
    last_event_timestamp
    
from daily_events