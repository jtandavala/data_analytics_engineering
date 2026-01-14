with orders_enriched as (
    select * from {{ ref('int_orders') }}
),
timing_metrics as (
    select
        order_id,
        order_purchase_timestamp,
        order_approved_at,
        order_delivered_carrier_date,
        order_delivered_customer_date,
        order_estimated_delivery_date,
        order_status,
        -- time in hours
        round(extract(epoch from (order_approved_at::timestamp - order_purchase_timestamp::timestamp)) / 3600::numeric, 0)::integer as approval_time_hours,
        round(extract(epoch from (order_delivered_carrier_date::timestamp - order_approved_at::timestamp)) / 3600::numeric, 0)::integer as carrier_handoff_time_hours,
        round(extract(epoch from (order_delivered_customer_date::timestamp - order_delivered_carrier_date::timestamp)) / 3600::numeric, 0)::integer as shipping_time_hours,
        round(extract(epoch from (order_delivered_customer_date::timestamp - order_purchase_timestamp::timestamp)) / 3600::numeric, 0)::integer as total_delivery_time_hours,
        round(extract(epoch from (order_estimated_delivery_date::timestamp - order_delivered_customer_date::timestamp)) / 3600::numeric, 0)::integer as delivery_delay_hours,
        -- Flags for performance
        case
            when order_delivered_customer_date::timestamp <= order_estimated_delivery_date::timestamp
            then 1
            else 0
        end as on_time_delivery,
        case
            when order_delivered_customer_date::timestamp > order_estimated_delivery_date::timestamp
            then 1
            else 0
        end as delivery_speed,
        -- Categorize delivery speed
        case
            when extract(epoch from (order_delivered_customer_date::timestamp - order_purchase_timestamp::timestamp)) / 3600 <= 24 then 'Express (<24h)'
            when extract(epoch from (order_delivered_customer_date::timestamp - order_purchase_timestamp::timestamp)) / 3600 <= 72 then 'Fast (24-72h)'
            when extract(epoch from (order_delivered_customer_date::timestamp - order_purchase_timestamp::timestamp)) / 3600 <= 168 then 'Standard (3-7d)'
            else 'Slow (>7d)'
        end as delivery_speed_category
    from orders_enriched
    where order_status = 'delivered'
        and order_delivered_customer_date is not null
)

select * from timing_metrics