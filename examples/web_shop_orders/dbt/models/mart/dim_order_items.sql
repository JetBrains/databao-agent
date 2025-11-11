with

order_items as (
    select * from {{ ref('stg__order_items') }}
),

order_items_subset as (
    select
        -- primary key
        order_item_id,

        --  attributes
        order_item_sequential_number,
        shipping_limit_date

    from order_items
)

select * from order_items_subset
