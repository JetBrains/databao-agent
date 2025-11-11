with

order_payments as (
    select * from {{ ref('stg__order_payments') }}
),


order_payments_subset as (
    select
        -- primary key
        payment_id,

        -- attributes
        payment_sequential_number,
        payment_type,
        payment_installments
        
    from order_payments

)

select * from order_payments_subset