{{ config(materialized='table') }}

SELECT
    order_id,
    payment_sequential,
    payment_type,
    payment_installments,
    payment_value,
    CURRENT_TIMESTAMP() AS last_extract_ts
FROM {{ source('Brazilian_Ecommerce', 'olist_order_payments_dataset') }}
LIMIT 1000