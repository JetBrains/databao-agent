with

order_reviews as (
    select * from {{ ref('stg__order_reviews') }}
),

order_reviews_subset as (
    select
        -- primary key
        review_id,
        
        -- attributes
        review_score,
        review_comment_title,
        review_comment_message,
        review_sent_at,
        review_answered_at

    from order_reviews

)

select * from order_reviews_subset
