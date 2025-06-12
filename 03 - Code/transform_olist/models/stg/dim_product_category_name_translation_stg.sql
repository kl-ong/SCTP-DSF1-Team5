SELECT
	SAFE_CAST(product_category_name AS STRING) AS product_category_name,
	SAFE_CAST(product_category_name_english AS STRING) AS product_category_name_english,
    CURRENT_TIMESTAMP() AS last_extract_ts
FROM {{ source('olist_raw', 'product_category_name_translation') }} 
WHERE 
	product_category_name IS NOT NULL
	AND product_category_name_english IS NOT NULL
	AND product_category_name <> ''
	AND product_category_name_english <> ''