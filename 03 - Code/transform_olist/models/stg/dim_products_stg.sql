SELECT 
    product_id,
    SAFE_CAST(product_category_name AS STRING) product_category_name,
    SAFE_CAST(product_name_lenght AS NUMERIC) product_name_lenght,
    SAFE_CAST(product_description_lenght AS NUMERIC) product_description_lenght,
    SAFE_CAST(product_photos_qty AS NUMERIC) product_photos_qty,
    SAFE_CAST(product_weight_g AS NUMERIC) product_weight_g,
    SAFE_CAST(product_length_cm AS NUMERIC) product_length_cm,
    SAFE_CAST(product_height_cm AS NUMERIC) product_height_cm,
    SAFE_CAST(product_width_cm AS NUMERIC) product_width_cm,
    CURRENT_TIMESTAMP() AS last_extract_ts
FROM {{ source('olist_raw', 'olist_products') }}
WHERE 
	product_id IS NOT NULL 
	AND product_category_name IS NOT NULL 
	AND product_name_lenght IS NOT NULL 
	AND product_description_lenght IS NOT NULL 
	AND product_photos_qty IS NOT NULL 
	AND product_weight_g IS NOT NULL 
	AND product_length_cm IS NOT NULL 
	AND product_height_cm IS NOT NULL 
	AND product_width_cm IS NOT NULL 
	AND product_id <> ''
	AND product_category_name <> ''