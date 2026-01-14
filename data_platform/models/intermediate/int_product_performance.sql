with order_items as (
    select * from {{ ref('stg_order_items') }}
),
products as (
    select * from {{ ref('stg_products') }}
),
orders_enriched as (
    select * from {{ ref('int_orders') }}
),
product_orders as (
    select
       oi.product_id,
       p.category_name,
       count(distinct oi.order_id) as order_count,
       count(distinct oi.seller_id) as seller_count,
       round(sum(oi.price)::numeric, 2) as total_revenue,
       round(sum(oi.freight_value)::numeric, 2) as total_freight,
       round(avg(oi.price)::numeric, 2) as avg_price,
       round(sum(oi.price + oi.freight_value)::numeric, 2) as total_order_value,
       count(*) as units_solds,
       round(sum(case when oe.is_delivered = 1 then 1 else 0 end)::numeric, 2) as units_delivered,
       round(sum(case when oe.is_delivered = 1 then oi.price else 0 end)::numeric, 2) as delivered_revenue
    from order_items oi
    inner join products p on oi.product_id = p.product_id
    left join orders_enriched oe on oi.order_id = oe.order_id
    group by oi.product_id, p.category_name
)
select
    product_id,
    category_name,
    order_count,
    seller_count,
    units_solds as units_sold,
    units_delivered,
    total_revenue,
    delivered_revenue,
    total_freight,
    total_order_value as total_value,
    avg_price,
     -- Taxa de entrega
    case
        when units_solds > 0
        then units_delivered::float / units_solds
        else 0
    end as delivery_rate,
     -- Revenue per order
    case 
        when order_count > 0 
        then total_revenue / order_count 
        else 0 
    end as revenue_per_order
from product_orders