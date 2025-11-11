with 

source as (
    select *
    from {{ source('webshop', 'customers') }}
),

refactored as (
    select
        -- primary key
        {{ dbt_utils.generate_surrogate_key(['customer_id']) }} as order_customer_id,

        -- foreign keys
        {{ dbt_utils.generate_surrogate_key(['customer_unique_id']) }}  as customer_id,

        -- attributes
        customer_zip_code_prefix,
        customer_city,
        customer_state

    from source
)

select * from refactored