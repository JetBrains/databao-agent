with

order_payments as (
    select * from {{ ref('stg__order_payments') }}
),

orders as (
    select * from {{ ref('stg__orders') }}
),


fact_order_payments as (
    select
        -- primary key
        order_payments.payment_id,

        -- foreign keys
        order_payments.order_id,

        -- order payment attributes
        order_payments.payment_value,

        -- order attributes
        orders.order_purchased_at

    from order_payments
    left join orders
        using(order_id)
)

select * from fact_order_payments