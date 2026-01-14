with reviews as (
    select * from {{ ref('stg_order_overviews') }}
),
orders_enriched as (
    select * from {{ ref('int_orders') }}
),
review_metrics as (
    select
        oe.order_id,
        oe.order_value,
        oe.product_count,
        r.review_id,
        r.review_score,
        r.review_comment_title,
        r.review_comment_message,
        r.review_creation_date,
        r.review_answer_timestamp,
        -- Tempo de resposta
        extract(epoch from (r.review_answer_timestamp::timestamp - r.review_creation_date::timestamp)) / 3600 as response_time_hours,
        -- Flags
        case when r.review_score >= 4 then 1 else 0 end as is_positive_review,
        case when r.review_score <= 2 then 1 else 0 end as is_negative_review,
        case when r.review_comment_message is not null then 1 else 0 end as has_comment
    from orders_enriched oe
    inner join reviews r on oe.order_id = r.order_id
    where r.review_score is not null
)
select * from review_metrics