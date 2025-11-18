# DuckDB schema overview (auto-generated)

This short note helps the LLM understand what's in the database.


## main.dim_customers

- customer_id: VARCHAR
- customer_zip_code_prefix: VARCHAR
- customer_city: VARCHAR
- customer_state: VARCHAR
- first_order_made_at: TIMESTAMP
- last_order_made_at: TIMESTAMP
- customer_latitude: DECIMAL(20,16)
- customer_longitude: DECIMAL(20,16)

## main.dim_order_items

- order_item_id: VARCHAR
- order_item_sequential_number: INTEGER
- shipping_limit_date: DATE

## main.dim_order_payments

- payment_id: VARCHAR
- payment_sequential_number: INTEGER
- payment_type: VARCHAR
- payment_installments: INTEGER

## main.dim_order_reviews

- review_id: VARCHAR
- review_score: INTEGER
- review_comment_title: VARCHAR
- review_comment_message: VARCHAR
- review_sent_at: DATE
- review_answered_at: TIMESTAMP

## main.dim_orders

- order_id: VARCHAR
- order_status: VARCHAR
- order_purchased_at: TIMESTAMP
- order_approved_at: TIMESTAMP
- order_delivered_to_carrier_at: TIMESTAMP
- order_delivered_to_customer_at: TIMESTAMP
- order_estimated_delivery_date: DATE

## main.dim_products

- product_id: VARCHAR
- product_category_name: VARCHAR
- product_name_length: INTEGER
- product_description_length: INTEGER
- product_photos_qty: INTEGER
- product_weight_g: INTEGER
- product_length_cm: INTEGER
- product_height_cm: INTEGER
- product_width_cm: INTEGER
- product_category_name_english: VARCHAR

## main.dim_sellers

- seller_id: VARCHAR
- seller_zip_code_prefix: VARCHAR
- seller_city: VARCHAR
- seller_state: VARCHAR
- seller_latitude: DECIMAL(20,16)
- seller_longitude: DECIMAL(20,16)

## main.fct_order_payments

- payment_id: VARCHAR
- order_id: VARCHAR
- payment_value: DECIMAL(10,2)
- order_purchased_at: TIMESTAMP

## main.fct_sales

- order_item_id: VARCHAR
- order_id: VARCHAR
- product_id: VARCHAR
- seller_id: VARCHAR
- customer_id: VARCHAR
- review_id: VARCHAR
- item_price: DECIMAL(10,2)
- item_freight_value: DECIMAL(10,2)
- order_purchased_at: TIMESTAMP

## main.metricflow_time_spine

- date_day: DATE

## main.stg__geolocations

- zip_code_prefix: VARCHAR
- latitude: DECIMAL(20,16)
- longitude: DECIMAL(20,16)
- city: VARCHAR
- state: VARCHAR

## main.stg__order_customers

- order_customer_id: VARCHAR
- customer_id: VARCHAR
- customer_zip_code_prefix: VARCHAR
- customer_city: VARCHAR
- customer_state: VARCHAR

## main.stg__order_items

- order_item_id: VARCHAR
- order_id: VARCHAR
- product_id: VARCHAR
- seller_id: VARCHAR
- order_item_sequential_number: INTEGER
- shipping_limit_date: DATE
- item_price: DECIMAL(10,2)
- item_freight_value: DECIMAL(10,2)

## main.stg__order_payments

- payment_id: VARCHAR
- order_id: VARCHAR
- payment_sequential_number: INTEGER
- payment_type: VARCHAR
- payment_installments: INTEGER
- payment_value: DECIMAL(10,2)

## main.stg__order_reviews

- review_id: VARCHAR
- order_id: VARCHAR
- review_score: INTEGER
- review_comment_title: VARCHAR
- review_comment_message: VARCHAR
- review_sent_at: DATE
- review_answered_at: TIMESTAMP

## main.stg__orders

- order_id: VARCHAR
- order_customer_id: VARCHAR
- order_status: VARCHAR
- order_purchased_at: TIMESTAMP
- order_approved_at: TIMESTAMP
- order_delivered_to_carrier_at: TIMESTAMP
- order_delivered_to_customer_at: TIMESTAMP
- order_estimated_delivery_date: DATE

## main.stg__product_category_name_translations

- product_category_name: VARCHAR
- product_category_name_english: VARCHAR

## main.stg__products

- product_id: VARCHAR
- product_category_name: VARCHAR
- product_name_length: INTEGER
- product_description_length: INTEGER
- product_photos_qty: INTEGER
- product_weight_g: INTEGER
- product_length_cm: INTEGER
- product_height_cm: INTEGER
- product_width_cm: INTEGER

## main.stg__sellers

- seller_id: VARCHAR
- seller_zip_code_prefix: VARCHAR
- seller_city: VARCHAR
- seller_state: VARCHAR