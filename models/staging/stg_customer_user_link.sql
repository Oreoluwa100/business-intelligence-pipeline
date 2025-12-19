{{
    config(
        schema='staging',
        materialized='view',
        tags=['staging', 'saas_platform']
    )
}}

select
    link_id,
    customer_id,
    user_id,
    link_type,
    confidence_score,
    created_date::date as created_date,
    case 
        when confidence_score >= 0.9 then 'High Confidence'
        when confidence_score >= 0.7 then 'Medium Confidence'
        else 'Low Confidence'
    end as confidence_level,
    case 
        when confidence_score < 0.5 then 'Questionable Link'
        when created_date > current_date then 'Future Date'
        else 'Valid Link'
    end as link_validation_status
from {{ source('saas_platform', 'customer_user_link') }}