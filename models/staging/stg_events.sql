{{
    config(
        schema='staging',
        materialized='view',
        tags=['staging', 'saas_platform']
    )
}}

select
    event_id,
    user_id,
    event_type,
    event_category,
    event_timestamp,
    event_date::date as event_date,
    extract(hour from event_timestamp) as event_hour,
    extract(isodow from event_timestamp) as event_day_of_week,
    event_properties,
    platform,
    session_id,
    (event_properties->>'view_duration_seconds')::integer as view_duration_seconds,
    event_properties->>'dashboard_id' as dashboard_id,
    event_properties->>'report_type' as report_type,
    event_properties->>'chart_type' as chart_type
from {{ source('saas_platform', 'events') }}