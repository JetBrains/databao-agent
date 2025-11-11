-- This is a technical model that generates a time spine for the last 10 years.

with 

days as (
    {{ dbt_date.get_base_dates(n_dateparts=365*10, datepart="day") }}

),

cast_to_date as (

    select 
        cast(date_day as date) as date_day
    from days
)

select * from cast_to_date