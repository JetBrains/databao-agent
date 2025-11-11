with

orders as (
    select * from {{ ref('stg__orders') }}
),

orders_subset as (
    select
        -- primary key
        order_id,
        
        -- attributes
        order_status,
        order_purchased_at,
        order_approved_at,
        order_delivered_to_carrier_at,
        order_delivered_to_customer_at,
        order_estimated_delivery_date

    from orders

)

select * from orders_subset
