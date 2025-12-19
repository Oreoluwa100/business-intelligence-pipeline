{{
    config(
        schema='staging',
        materialized='view',
        tags=['staging', 'ecommerce']
    )
}}

select
    order_item_id,
    order_id,
    product_id,
    quantity,
    price,
    item_total
from {{ source('ecommerce', 'order_items') }}