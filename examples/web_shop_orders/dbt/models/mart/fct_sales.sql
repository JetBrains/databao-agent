with

order_items as (
    select * from {{ ref('stg__order_items') }}
),

orders as (
    select * from {{ ref('stg__orders') }}
),

order_reviews as (
    select * from {{ ref('stg__order_reviews') }}
),

order_customers as (
    select * from {{ ref('stg__order_customers') }}
),

order_reviews_deduplication_setup as (
    select
        *,
        row_number() over (partition by order_id order by review_answered_at desc) as review_rank
    from order_reviews
),

order_reviews_deduplicated as (
    select *
    from order_reviews_deduplication_setup
    where review_rank = 1
),


fact_sales as (
    select
        -- primary key
        order_items.order_item_id,

        -- foreign keys
        order_items.order_id,
        order_items.product_id,
        order_items.seller_id,
        order_customers.customer_id,
        order_reviews_deduplicated.review_id,

        -- order item attributes
        order_items.item_price,
        order_items.item_freight_value,

        -- order attributes
        orders.order_purchased_at

        
    from order_items
    left join orders
        using(order_id)
    left join order_reviews_deduplicated
        using(order_id)
    left join order_customers
        using(order_customer_id)
)

select * from fact_sales
