{{
    config(
        schema='analytics',
        materialized='table',
        tags=['analytics', 'ecommerce', 'customers', 'orders', 'products']
    )
}}

with customer_orders as (
    select
        customer_id,
        count(distinct order_id) as total_orders,
        sum(items_total) as lifetime_value,
        round(avg(items_total), 2) as avg_order_value,
        min(order_date) as first_order_date,
        max(order_date) as last_order_date,
        max(order_date) - min(order_date) as customer_lifetime_days,
        sum(item_count) as total_items_purchased
    from {{ ref('fct_orders') }}
    where order_status != 'cancelled'
    group by customer_id
),

customers as (
    select * from {{ ref('dim_customers') }}
)

select
    c.customer_id,
    c.email,
    c.first_name,
    c.last_name,
    c.signup_date,
    c.country_code,
    c.region_group,
    c.is_active_flag,
    c.days_since_signup,
    
    coalesce(co.total_orders, 0) as total_orders,
    coalesce(co.lifetime_value, 0) as customer_lifetime_value,
    coalesce(co.avg_order_value, 0) as avg_order_value,
    co.first_order_date,
    co.last_order_date,
    coalesce(co.customer_lifetime_days, 0) as customer_lifetime_days,
    coalesce(co.total_items_purchased, 0) as total_items_purchased,
    
    case 
        when co.total_orders is null then 'Never Purchased'
        when co.total_orders = 1 then 'One-Time Buyer'
        else 'Purchased More Than Once'
    end as customer_segment,
    current_date - co.last_order_date as days_since_last_order

from customers c
left join customer_orders co on c.customer_id = co.customer_id