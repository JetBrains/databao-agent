with 

source as (
    select *
    from {{ source('webshop', 'orders') }}
),

refactored as (
    select
        -- primary key
        {{ dbt_utils.generate_surrogate_key(['order_id']) }} as order_id,
        
        -- foreign keys
        {{ dbt_utils.generate_surrogate_key(['customer_id']) }}  as order_customer_id,
        
        -- attributes
        order_status,
        order_purchase_timestamp as order_purchased_at,
        order_approved_at,
        order_delivered_carrier_date as order_delivered_to_carrier_at,
        order_delivered_customer_date as order_delivered_to_customer_at,
        order_estimated_delivery_date::date as order_estimated_delivery_date
        
    from source
)

select * from refactored