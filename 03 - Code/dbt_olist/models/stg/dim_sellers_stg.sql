{{ config(materialized='table') }}

SELECT
    seller_id,
    seller_zip_code_prefix,
    seller_city,
    seller_state,
    CURRENT_TIMESTAMP() AS last_extract_ts
FROM {{ source('Brazilian_Ecommerce', 'olist_sellers_dataset') }}