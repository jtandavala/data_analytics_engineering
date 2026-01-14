-- models/marts/logistics/fact_order_fulfillment.sql
{{
    config(
        materialized='table',
        description='Order fulfillment metrics for logistics analysis',
        tags=['logistics', 'fact', 'fulfillment']
    )
}}

with orders as (
    select * from {{ ref('int_orders') }}
),
date_dim as (
    select * from {{ ref('dim_date') }}
)

select
    o.order_id,
    o.customer_id,
    date(o.order_purchase_timestamp) as order_date,
    dd.date_key,
    
    -- Timestamps
    o.order_purchase_timestamp,
    o.order_approved_at,
    o.order_delivered_carrier_date,
    o.order_delivered_customer_date,
    o.order_estimated_delivery_date,
    
    -- Time calculations (in days)
    extract(epoch from (o.order_approved_at::timestamp - o.order_purchase_timestamp::timestamp)) / 86400 as approval_time_days,
    extract(epoch from (o.order_delivered_carrier_date::timestamp - o.order_approved_at::timestamp)) / 86400 as processing_time_days,
    extract(epoch from (o.order_delivered_customer_date::timestamp - o.order_delivered_carrier_date::timestamp)) / 86400 as shipping_time_days,
    extract(epoch from (o.order_delivered_customer_date::timestamp - o.order_purchase_timestamp::timestamp)) / 86400 as total_fulfillment_time_days,
    extract(epoch from (o.order_estimated_delivery_date::timestamp - o.order_delivered_customer_date::timestamp)) / 86400 as delivery_variance_days,
    
    -- SLA metrics
    case
        when o.order_delivered_customer_date is not null
             and o.order_delivered_customer_date <= o.order_estimated_delivery_date
        then true
        else false
    end as on_time_delivery,
    
    case
        when o.order_delivered_customer_date is not null
             and o.order_delivered_customer_date > o.order_estimated_delivery_date
        then true
        else false
    end as late_delivery,
    
    -- Status
    o.order_status,
    o.is_delivered,
    o.is_canceled,
    
    -- Order metrics
    o.order_value,
    o.product_count,
    o.seller_count

from orders o
inner join date_dim dd on date(o.order_purchase_timestamp) = dd.date
where o.order_purchase_timestamp is not null