{{ config(materialized='table') }}

SELECT 
    product_id,
    product_category_name,
    product_name_lenght,
    product_description_lenght,
    product_photos_qty,
    product_weight_g,
    product_length_cm,
    product_height_cm,
    product_width_cm,
    CURRENT_TIMESTAMP() AS last_extract_ts
FROM {{ source('Brazilian_Ecommerce', 'olist_products_dataset') }} 