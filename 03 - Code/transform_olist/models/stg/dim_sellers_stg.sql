SELECT
    seller_id,
    SAFE_CAST(seller_zip_code_prefix AS STRING) seller_zip_code_prefix,
    SAFE_CAST(seller_city AS STRING) seller_city,
    SAFE_CAST(seller_state AS STRING) seller_state,
    CURRENT_TIMESTAMP() AS last_extract_ts
FROM {{ source('olist_raw', 'olist_sellers') }}
WHERE 
	seller_id IS NOT NULL 
	AND seller_zip_code_prefix IS NOT NULL
	AND seller_city IS NOT NULL
	AND seller_state IS NOT NULL 
	AND seller_id <> '' 
	AND seller_city <> ''
	AND seller_state <> ''