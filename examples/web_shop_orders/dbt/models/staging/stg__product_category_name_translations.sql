with 

source as (
    select *
    from {{ source('webshop', 'product_category_name_translation') }}
),

refactored as (
    select
        -- primary key
        product_category_name,
        
        -- attributes
        product_category_name_english
        
    from source
)

select * from refactored