with 

source as (
    select *
    from {{ source('webshop', 'sellers') }}
),

refactored as (
    select
        -- primary key
        {{ dbt_utils.generate_surrogate_key(['seller_id']) }} as seller_id,
        
        -- attributes
        seller_zip_code_prefix,
        seller_city,
        seller_state
    from source
)

select * from refactored