{{ config(materialized='table') }}

SELECT
	CAST(string_field_0 AS STRING) AS product_category_name,
	CAST(string_field_1 AS STRING) AS product_category_name_english,
    CURRENT_TIMESTAMP() AS last_extract_ts
FROM {{ source('Brazilian_Ecommerce', 'product_category_name_translation') }} 
WHERE 
	string_field_0 IS NOT NULL
	AND string_field_1 IS NOT NULL
	AND string_field_0 <> ''
	AND string_field_1 <> ''