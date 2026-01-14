with sellers as (
    select * from {{ ref('stg_sellers') }}
),
order_items as (
    select * from {{ ref('stg_order_items') }}
),
orders_enriched as (
    select * from {{ ref('int_orders') }}
),
seller_orders as (
    select
        s.seller_id,
        s.seller_city,
        s.seller_state,
        s.seller_zip_code_prefix,
        -- Volume
        count(distinct oi.order_id) as total_orders,
        count(distinct oi.product_id) as unique_products,
        count(*) as total_items_sold,
        -- Revenue
        round(sum(oi.price)::numeric, 2) as total_revenue,
        round(sum(oi.freight_value)::numeric, 2) as total_freight,
        round(sum(oi.price + oi.freight_value)::numeric, 2) as total_value,
        -- Performance
        round(sum(case when oe.is_delivered = 1 then oi.price else 0 end)::numeric, 2) as delivered_revenue,
        count(distinct case when oe.is_delivered = 1 then oi.order_id end) as delivered_orders,
        -- Prices
        round(avg(oi.price)::numeric, 2) as avg_price,
        round(percentile_cont(0.5) within group (order by oi.price)::numeric, 2) as median_item_price
    from sellers s
    left join order_items oi on s.seller_id = oi.seller_id
    left join orders_enriched oe on oi.order_id = oe.order_id
    group by 
        s.seller_id,
        s.seller_city,
        s.seller_state,
        s.seller_zip_code_prefix
)

select * from seller_orders