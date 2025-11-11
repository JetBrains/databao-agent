with 

source as (
    select *
    from {{ source('webshop', 'order_reviews') }}
),

refactored as (
    select
    distinct on (review_id)
        -- primary key
        {{ dbt_utils.generate_surrogate_key(['review_id']) }} as review_id,

        -- foreign keys
        {{ dbt_utils.generate_surrogate_key(['order_id']) }} as order_id,
        
        -- attributes
        review_score,
        review_comment_title,
        review_comment_message,
        review_creation_date as review_sent_at,
        review_answer_timestamp as review_answered_at
        
    from source
    order by review_id, review_answer_timestamp desc
)

select * from refactored