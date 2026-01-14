-- models/marts/logistics/logistics_kpis.sql
{{
    config(
        materialized='table',
        description='Logistics KPIs for BI and AI consumption',
        tags=['logistics', 'kpis', 'ai-ready']
    )
}}

with fulfillment as (
    select * from {{ ref('fact_order_fulfillment') }}
),
daily_metrics as (
    select
        order_date,
        count(distinct order_id) as total_orders,
        count(distinct case when is_delivered = 1 then order_id end) as delivered_orders,
        
        -- Average times
        avg(approval_time_days) as avg_approval_time,
        avg(processing_time_days) as avg_processing_time,
        avg(shipping_time_days) as avg_shipping_time,
        avg(total_fulfillment_time_days) as avg_total_fulfillment_time,
        
        -- SLA compliance
        sum(case when on_time_delivery = true then 1 else 0 end)::float / 
        nullif(count(distinct case when is_delivered = 1 then order_id end), 0) as on_time_delivery_rate,
        
        -- Late deliveries
        sum(case when late_delivery = true then 1 else 0 end) as late_deliveries,
        sum(case when late_delivery = true then 1 else 0 end)::float / 
        nullif(count(distinct case when is_delivered = 1 then order_id end), 0) as late_delivery_rate

    from fulfillment
    group by order_date
)

select
    *,
    -- Performance classification
    case
        when on_time_delivery_rate >= 0.95 then 'Excellent'
        when on_time_delivery_rate >= 0.90 then 'Good'
        when on_time_delivery_rate >= 0.80 then 'Fair'
        else 'Needs Improvement'
    end as delivery_performance_status,
    
    -- Target vs actual
    case
        when avg_total_fulfillment_time <= 10 then 'Fast'
        when avg_total_fulfillment_time <= 20 then 'Normal'
        else 'Slow'
    end as fulfillment_speed_category

from daily_metrics
order by order_date desc