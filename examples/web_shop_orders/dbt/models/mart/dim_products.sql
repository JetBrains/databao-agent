with

products as (
    select * from {{ ref('stg__products') }}
),

product_category_name_translations as (
    select * from {{ ref('stg__product_category_name_translations') }}
),

product_enriched as (
    select
        -- primary key
        product_id,

        -- product attributes
        products.product_category_name,
        products.product_name_length,
        products.product_description_length,
        products.product_photos_qty,
        products.product_weight_g,
        products.product_length_cm,
        products.product_height_cm,
        products.product_width_cm,

        -- name_translations attributes
        product_category_name_translations.product_category_name_english


    from products
    left join product_category_name_translations
        using(product_category_name)
)

select * from product_enriched
