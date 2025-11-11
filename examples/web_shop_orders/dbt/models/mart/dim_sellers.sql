with

sellers as (
    select * from {{ ref('stg__sellers') }}
),

geolocations as (
    select * from {{ ref('stg__geolocations') }}
),


sellers_enriched as (
    select
        -- primary key
        sellers.seller_id,

        -- seller attributes
        sellers.seller_zip_code_prefix,
        sellers.seller_city,
        sellers.seller_state,

        -- geolocation attributes
        geolocations.latitude as seller_latitude,
        geolocations.longitude as seller_longitude

    from sellers
    left join geolocations 
        on sellers.seller_zip_code_prefix = geolocations.zip_code_prefix
)

select * from sellers_enriched
