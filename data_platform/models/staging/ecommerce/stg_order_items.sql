select
    order_id,
    product_id,
    seller_id,
    price,
    freight_value
from {{ source('olist_raw', 'order_items') }}
