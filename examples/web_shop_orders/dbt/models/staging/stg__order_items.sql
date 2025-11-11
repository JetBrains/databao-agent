with 

source as (
    select *
    from {{ source('webshop', 'order_items') }}
),

refactored as (
    select
        -- primary key
        {{ dbt_utils.generate_surrogate_key(['order_id', 'order_item_id']) }} as order_item_id,

        -- foreign keys
        {{ dbt_utils.generate_surrogate_key(['order_id']) }} as order_id,
        {{ dbt_utils.generate_surrogate_key(['product_id']) }} as product_id,
        {{ dbt_utils.generate_surrogate_key(['seller_id']) }} as seller_id,
        
        -- attributes
        order_item_id as order_item_sequential_number,

        shipping_limit_date::date as shipping_limit_date,
        price as item_price,
        freight_value as item_freight_value
        
    from source
)

select * from refactored