-- models/marts/sales/fact_sales_daily.sql
{{
    config(
        materialized='table',
        description='Daily sales metrics for sales reporting',
        tags=['sales', 'fact', 'daily']
    )
}}

with fact_orders as (
    select * from {{ ref('fact_orders') }}
),
date_dim as (
    select * from {{ ref('dim_date') }}
),
daily_sales as (
    select
        order_date,
        product_category,
        customer_state,
        seller_state,
        
        -- Volume
        count(distinct order_id) as orders_count,
        count(distinct customer_id) as customers_count,
        count(distinct product_id) as products_count,
        count(distinct seller_id) as sellers_count,
        sum(item_quantity) as items_sold,
        
        -- Revenue
        sum(item_total_value) as total_revenue,
        sum(case when is_delivered = 1 then item_total_value else 0 end) as delivered_revenue,
        sum(case when is_canceled = 1 then item_total_value else 0 end) as canceled_revenue,
        
        -- Averages
        avg(item_total_value) as avg_item_value,
        sum(item_total_value) / nullif(count(distinct order_id), 0) as avg_order_value
        
    from fact_orders
    group by order_date, product_category, customer_state, seller_state
)

select
    ds.order_date,
    dd.date_key,
    dd.year,
    dd.quarter,
    dd.month,
    ds.product_category,
    ds.customer_state,
    ds.seller_state,
    
    -- Metrics
    ds.orders_count,
    ds.customers_count,
    ds.products_count,
    ds.sellers_count,
    ds.items_sold,
    ds.total_revenue,
    ds.delivered_revenue,
    ds.canceled_revenue,
    ds.avg_item_value,
    ds.avg_order_value,
    
    -- Conversion rates
    case
        when ds.orders_count > 0
        then ds.delivered_revenue / ds.total_revenue
        else 0
    end as delivery_conversion_rate,
    
    case
        when ds.orders_count > 0
        then ds.canceled_revenue / ds.total_revenue
        else 0
    end as cancellation_rate

from daily_sales ds
inner join date_dim dd on ds.order_date = dd.date