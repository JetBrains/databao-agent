with 

source as (
    select *
    from {{ source('webshop', 'products') }}
),

refactored as (
    select
        -- primary key
        {{ dbt_utils.generate_surrogate_key(['product_id']) }}  as product_id,
        
        -- foreign keys
        product_category_name,
        
        -- attributes
        product_name_lenght as product_name_length,
        product_description_lenght as product_description_length,
        product_photos_qty,
        product_weight_g,
        product_length_cm,
        product_height_cm,
        product_width_cm
        
    from source
)

select * from refactored