-- models/marts/customer_analytics/fact_customer_behavior.sql
{{
    config(
        materialized='table',
        description='Customer behavior metrics for analytics',
        tags=['customer', 'analytics', 'fact']
    )
}}

with customer_metrics as (
    select * from {{ ref('int_customer_metrics') }}
),
customers as (
    select * from {{ ref('stg_customers') }}
)

select
    c.customer_id,
    c.customer_unique_id,
    c.customer_city,
    c.customer_state,
    
    -- RFM metrics
    cm.order_count as frequency,
    cm.days_since_last_order as recency_days,
    cm.delivered_lifetime_value as monetary_value,
    
    -- Customer segments
    cm.customer_segment,
    
    -- Value metrics
    cm.total_lifetime_value,
    cm.delivered_lifetime_value,
    cm.avg_order_value,
    cm.max_order_value,
    
    -- Time metrics
    cm.first_order_date,
    cm.last_order_date,
    cm.customer_lifetime_days,
    cm.estimated_monthly_value,
    
    -- RFM scores (1-5 scale)
    case
        when cm.order_count >= 5 then 5
        when cm.order_count >= 3 then 4
        when cm.order_count >= 2 then 3
        when cm.order_count = 1 then 2
        else 1
    end as frequency_score,
    
    case
        when cm.days_since_last_order <= 30 then 5
        when cm.days_since_last_order <= 60 then 4
        when cm.days_since_last_order <= 90 then 3
        when cm.days_since_last_order <= 180 then 2
        else 1
    end as recency_score,
    
    case
        when cm.delivered_lifetime_value >= 1000 then 5
        when cm.delivered_lifetime_value >= 500 then 4
        when cm.delivered_lifetime_value >= 200 then 3
        when cm.delivered_lifetime_value >= 100 then 2
        else 1
    end as monetary_score,
    
    -- Combined RFM
    (case
        when cm.order_count >= 5 then 5
        when cm.order_count >= 3 then 4
        when cm.order_count >= 2 then 3
        when cm.order_count = 1 then 2
        else 1
    end::text || 
    case
        when cm.days_since_last_order <= 30 then 5
        when cm.days_since_last_order <= 60 then 4
        when cm.days_since_last_order <= 90 then 3
        when cm.days_since_last_order <= 180 then 2
        else 1
    end::text ||
    case
        when cm.delivered_lifetime_value >= 1000 then 5
        when cm.delivered_lifetime_value >= 500 then 4
        when cm.delivered_lifetime_value >= 200 then 3
        when cm.delivered_lifetime_value >= 100 then 2
        else 1
    end::text) as rfm_code

from customers c
left join customer_metrics cm on c.customer_id = cm.customer_id