{{
    config(
        schema='staging',
        materialized='view',
        tags=['staging', 'ecommerce', 'products']
    )
}}

select
    product_id,
    product_name,
    category,
    price,
    case 
        when category = 'Laptops' then 'Computing'
        when category = 'Phones' then 'Mobile'
        when category = 'Tablets' then 'Mobile'
        else 'Accessories'
    end as product_family
from {{ source('ecommerce', 'products') }}