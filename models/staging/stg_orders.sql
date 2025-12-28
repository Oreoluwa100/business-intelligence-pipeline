{{
    config(
        schema='staging',
        materialized='view',
        tags=['staging', 'ecommerce', 'orders']
    )
}}

select
    order_id,
    customer_id,
    order_date::timestamp as order_timestamp,
    order_date::date as order_date,
    extract(year from order_date) as order_year,
    extract(month from order_date) as order_month,
    extract(day from order_date) as order_day,
    extract(isodow from order_date) as order_day_of_week,
    lower(status) as order_status,
    total_amount,
    case 
        when total_amount >= 1000 then 'Large Order'
        when total_amount >= 100 then 'Medium Order'
        else 'Small Order'
    end as order_size,
    case 
        when total_amount < 0 then 'Invalid Negative Amount'
        when order_date > current_timestamp then 'Future Order Date'
        else 'Valid'
    end as order_validation_status
from {{ source('ecommerce', 'orders') }}