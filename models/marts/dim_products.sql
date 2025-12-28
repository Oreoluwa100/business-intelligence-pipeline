{{
    config(
        schema='marts',
        materialized='table',
        tags=['marts', 'ecommerce', 'dimension', 'products']
    )
}}

select
    product_id,
    product_name,
    category,
    price,
    product_family
from {{ ref('stg_products') }}