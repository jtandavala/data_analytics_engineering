with orders as (
    select * from {{ ref('stg_orders') }}
),
order_items_agg as (
    select
        order_id,
        count(distinct product_id) as product_count,
        count(distinct seller_id) as seller_count,
        sum(price) as total_amount,
        sum(freight_value) as total_freight_amount,
        round(sum(price + freight_value)::numeric, 2) as total_order_amount
    from {{ ref('stg_order_items') }}
    group by order_id
),
order_payments_agg as (
    select
        order_id,
        round(sum(payment_value)::numeric, 2) as total_payment_amount,
        count(distinct payment_type) as payment_methods_count,
        max(payment_installments) as max_installments,
        string_agg(distinct payment_type, ', ') as payment_types
    from {{ ref('stg_order_payments') }}
    group by order_id
),
payments_by_type as (
    select
        order_id,
        payment_type,
        round(sum(payment_value)::numeric, 2) as payment_amount
    from {{ ref('stg_order_payments') }}
    group by order_id, payment_type
),
payment_type_totals as (
    select
        order_id,
        sum(case when payment_type = 'credit_card' then payment_amount else 0 end) as total_credit_card_amount,
        sum(case when payment_type = 'voucher' then payment_amount else 0 end) as total_voucher_amount,
        sum(case when payment_type = 'boleto' then payment_amount else 0 end) as total_boleto_amount,
        sum(case when payment_type = 'debit_card' then payment_amount else 0 end) as total_debit_card_amount
    from payments_by_type
    group by order_id
),
final as (
    select
        o.order_id,
        o.customer_id,
        o.order_status,
        op.payment_types,
        coalesce(pt.total_credit_card_amount, 0) as total_credit_card_amount,
        coalesce(pt.total_boleto_amount, 0) as total_boleto_amount,
        coalesce(pt.total_voucher_amount, 0) as total_voucher_amount,
        coalesce(pt.total_debit_card_amount, 0) as total_debit_card_amount,
        coalesce(oi.product_count, 0) as product_count,
        coalesce(oi.seller_count, 0) as seller_count,
        coalesce(oi.total_order_amount, 0) as order_value,
        coalesce(oi.total_amount, 0) as items_total,
        coalesce(oi.total_freight_amount, 0) as freight_total,
        coalesce(op.total_payment_amount, 0) as payment_total,
        op.payment_methods_count,
        o.order_purchase_timestamp,
        o.order_approved_at,
        o.order_delivered_carrier_date,
        o.order_delivered_customer_date,
        o.order_estimated_delivery_date,
        case when o.order_status = 'delivered' then 1 else 0 end as is_delivered,
        case when o.order_status in ('canceled', 'unavailable') then 1 else 0 end as is_canceled
    from orders as o
    left join order_items_agg as oi on o.order_id = oi.order_id
    left join order_payments_agg as op on o.order_id = op.order_id
    left join payment_type_totals as pt on o.order_id = pt.order_id
)
select * from final