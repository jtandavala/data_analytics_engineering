{{
    config(
        materialized='table',
        description='Customer dimension with enriched attributes and segments',
        tags=['core', 'dimension', 'conformed']
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
    c.zip_code_prefix,
    
    -- Enriched metrics
    coalesce(cm.order_count, 0) as total_orders,
    coalesce(cm.delivered_order_count, 0) as delivered_orders,
    coalesce(cm.total_lifetime_value, 0) as lifetime_value,
    coalesce(cm.delivered_lifetime_value, 0) as delivered_lifetime_value,
    coalesce(cm.avg_order_value, 0) as avg_order_value,
    coalesce(cm.max_order_value, 0) as max_order_value,

    -- Paying customer flag
    coalesce(cm.is_paying_customer, false) as is_paying_customer,
    
    -- RFM-like attributes
    cm.days_since_last_order,
    cm.customer_lifetime_days,
    cm.customer_segment,
    cm.estimated_monthly_value,
    
    -- Effective dates for SCD Type 2 (if needed in future)
    current_timestamp as effective_from,
    null as effective_to,
    true as is_current

from customers c
left join customer_metrics cm on c.customer_id = cm.customer_id