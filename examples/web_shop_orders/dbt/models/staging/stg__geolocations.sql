with 

source as (
    select *
    from {{ source('webshop', 'geolocation') }}
),

refactored as (
    select distinct on (geolocation_zip_code_prefix)
        -- primary key
        geolocation_zip_code_prefix as zip_code_prefix,

        -- attributes
        geolocation_lat as latitude,
        geolocation_lng as longitude,
        geolocation_city as city,
        geolocation_state as state

    from source
)

select * from refactored