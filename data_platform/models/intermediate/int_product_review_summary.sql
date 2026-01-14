with order_items as (
    select * from {{ ref('stg_order_items') }}
),
review_metrics as (
    select * from {{ ref('int_review_metrics') }}
),
product_reviews as (
    select
        oi.product_id,
        count(distinct rm.order_id) as reviewed_orders,
        avg(rm.review_score) as avg_review_score,
        percentile_cont(0.5) within group (order by rm.review_score) as median_review_score,
        count(distinct case when rm.is_positive_review = 1 then rm.order_id end) as positive_reviews,
        count(distinct case when rm.is_negative_review = 1 then rm.order_id end) as negative_reviews,
        avg(rm.response_time_hours) as avg_response_time_hours,
        count(distinct case when rm.has_comment = 1 then rm.order_id end) as reviews_with_comments
    from order_items oi
    inner join review_metrics rm on oi.order_id = rm.order_id
    group by oi.product_id
)
select
    product_id,
    reviewed_orders,
    avg_review_score,
    median_review_score,
    positive_reviews,
    negative_reviews,
    reviews_with_comments,
    avg_response_time_hours,
    -- NPS-like score
    case 
        when reviewed_orders > 0 
        then ((positive_reviews::float - negative_reviews::float) / reviewed_orders) * 100
        else 0
    end as net_promoter_score,
    -- Taxa de satisfação
    case 
        when reviewed_orders > 0 
        then positive_reviews::float / reviewed_orders 
        else 0
    end as satisfaction_rate
from product_reviews