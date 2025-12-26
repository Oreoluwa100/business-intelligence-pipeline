{{
    config(
        schema='marts',
        materialized='incremental',
        unique_key='order_id',
        tags=['marts', 'ecommerce', 'fact', 'incremental']
    )
}}

with orders as (
    select * from {{ ref('stg_orders') }}
),

order_items as (
    select * from {{ ref('stg_order_items') }}
),

customers as (
    select * from {{ ref('dim_customers') }}
),

products as (
    select * from {{ ref('dim_products') }}
),

order_enriched as (
    select
        o.order_id,
        o.customer_id,
        o.order_date,
        o.order_timestamp,
        o.order_status,
        o.order_size,
        c.country_code as customer_country,
        c.region_group as customer_region,
        c.is_active_flag as customer_active_status,
        c.days_since_signup,
        c.signup_month,
        
        count(oi.order_item_id) as item_count,
        sum(oi.quantity) as total_quantity,
        sum(oi.item_total) as items_total,
        
        count(distinct p.category) as distinct_categories,

        case 
            when extract(hour from o.order_timestamp) between 9 and 17 
            then 'Business Hours'
            else 'After Hours'
        end as order_time_segment

    from orders o
    left join order_items oi on o.order_id = oi.order_id
    left join customers c on o.customer_id = c.customer_id
    left join products p on oi.product_id = p.product_id
    where o.order_validation_status = 'Valid'
    
    {% if is_incremental() %}
        and o.order_date > (select max(order_date) from {{ this }})
    {% endif %}
    
    group by 
        o.order_id,
        o.customer_id,
        o.order_date,
        o.order_timestamp,
        o.order_status,
        o.order_size,
        c.country_code,
        c.region_group,
        c.is_active_flag,
        c.days_since_signup,
        c.signup_month
)

select *
from order_enriched


