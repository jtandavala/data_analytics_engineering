with orders as (
    select * from {{ ref('int_orders') }}
),
order_items as (
    select * from {{ ref('stg_order_items') }}
),
products as (
    select * from {{ ref('stg_products') }}
),
customers as (
    select * from {{ ref('stg_customers') }}
),
sellers as (
    select * from {{ ref('stg_sellers') }}
)
select
    -- Surrogate keys (FKs to dimensions)
    oi.order_id || '_' || oi.product_id || '_' || oi.seller_id as order_item_key,
    o.customer_id,
    oi.product_id,
    oi.seller_id,
    date(o.order_purchase_timestamp) as order_date,
    c.customer_state,
    c.customer_city,
    s.seller_state,
    s.seller_city,
    
    -- Degenerate dimensions (stored in fact)
    o.order_id,
    o.order_status,
    o.payment_types,
    
    -- Measures (additive facts)
    oi.price as item_price,
    oi.freight_value as item_freight,
    oi.price + oi.freight_value as item_total_value,
    
    -- Semi-additive measures
    o.order_value as order_total_value,
    o.items_total as order_items_total,
    o.freight_total as order_freight_total,
    o.payment_total as order_payment_total,
    
    -- Payment breakdown (non-additive, but useful for analysis)
    o.total_credit_card_amount,
    o.total_boleto_amount,
    o.total_voucher_amount,
    o.total_debit_card_amount,
    
    -- Counts
    1 as item_quantity,
    o.product_count as order_product_count,
    o.seller_count as order_seller_count,
    
    -- Flags
    o.is_delivered,
    o.is_canceled,
    
    -- Timestamps (for time intelligence)
    o.order_purchase_timestamp,
    o.order_approved_at,
    o.order_delivered_carrier_date,
    o.order_delivered_customer_date,
    o.order_estimated_delivery_date,
    
    -- Product attributes (denormalized for performance)
    p.category_name as product_category,
    p.weight_g as product_weight_g,
    p.length_cm as product_length_cm,
    p.height_cm as product_height_cm,
    p.width_cm as product_width_cm

from order_items oi
inner join orders o on oi.order_id = o.order_id
inner join products p on oi.product_id = p.product_id
inner join customers c on o.customer_id = c.customer_id
inner join sellers s on oi.seller_id = s.seller_id