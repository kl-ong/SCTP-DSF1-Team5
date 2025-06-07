SELECT
    seller_id,
    seller_zip_code_prefix,
    seller_city,
    seller_state
FROM {{ source('Brazilian_Ecommerce', 'olist_sellers_dataset') }}