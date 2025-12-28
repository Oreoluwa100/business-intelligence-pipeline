{{
    config(
        schema='analytics',
        materialized='table',
        tags=['analytics', 'ecommerce', 'products', 'orders']
    )
}}

with orders_date as (
    select 
        order_id,
        customer_id,
        order_date
    from {{ ref('stg_orders') }}    
),

order_items as (
    select
        order_item_id,
        order_id,
        product_id,
        quantity,
        price,
        item_total
    from {{ ref('stg_order_items') }}
),

agg_orders as (
    select 
        oi.order_item_id,
        oi.order_id,
        oi.product_id,
        oi.quantity,
        oi.price,
        oi.item_total,
        od.customer_id,
        od.order_date
    from order_items oi left join orders_date od
    on oi.order_id = od.order_id
),

product_sales as (
    select 
        product_id,
        count(distinct order_id) as times_ordered,
        sum(quantity) as total_units_sold,
        sum(item_total) as total_amount,
        count(distinct customer_id) as unique_customers,
        min(order_date) as first_sale_date,
        max(order_date) as last_sale_date
    from agg_orders
    group by product_id
),

products as (
    select * from {{ ref('dim_products') }}
)

select
    p.product_id,
    p.product_name,
    p.category,
    p.product_family,
    p.price as list_price,

    coalesce(ps.times_ordered, 0) as times_ordered,
    coalesce(ps.total_units_sold, 0) as total_units_sold,
    coalesce(ps.total_amount, 0) as total_amount,
    coalesce(ps.unique_customers, 0) as unique_customers,
    ps.first_sale_date,
    ps.last_sale_date

from products p left join product_sales ps 
on p.product_id = ps.product_id

