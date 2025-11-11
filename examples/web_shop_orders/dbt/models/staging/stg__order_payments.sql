with 

source as (
    select *
    from {{ source('webshop', 'order_payments') }}
),

refactored as (
    select
        -- primary key
        {{ dbt_utils.generate_surrogate_key(['order_id', 'payment_sequential']) }} as payment_id,

        -- foreign keys
        {{ dbt_utils.generate_surrogate_key(['order_id']) }} as order_id,
        
        -- attributes
        payment_sequential as payment_sequential_number,
        payment_type,
        payment_installments,
        payment_value
        
    from source
)

select * from refactored