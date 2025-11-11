with

order_customers as (
    select * from {{ ref('stg__order_customers') }}
),

orders as (
    select * from {{ ref('stg__orders') }}
),

geolocations as (
    select * from {{ ref('stg__geolocations') }}
),

customers_deduplication_setup as (
    select
        order_customers.*,
        row_number() over(partition by order_customers.customer_id order by order_purchased_at desc) as row_number
    from order_customers
    left join orders using(order_customer_id)
),

customers_deduplicated as (
    select * from customers_deduplication_setup where row_number = 1
),

customers_metrics as (
    select
        order_customers.customer_id,
        min(orders.order_purchased_at) as first_order_made_at,
        max(orders.order_purchased_at) as last_order_made_at
    from order_customers
    left join orders using(order_customer_id)
    group by customer_id
),

customers_final as (
    select
        --primary key
        customers_deduplicated.customer_id,

        --customers attributes
        customers_deduplicated.customer_zip_code_prefix,
        customers_deduplicated.customer_city,
        customers_deduplicated.customer_state,
        customers_metrics.first_order_made_at,
        customers_metrics.last_order_made_at,

        --geolocation attributes
        geolocations.latitude as customer_latitude,
        geolocations.longitude as customer_longitude

    from customers_deduplicated
    left join customers_metrics using(customer_id)
    left join geolocations 
        on customers_deduplicated.customer_zip_code_prefix = geolocations.zip_code_prefix
)

select * from customers_final
