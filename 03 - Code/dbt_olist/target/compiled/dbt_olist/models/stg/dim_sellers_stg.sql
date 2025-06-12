

SELECT
    seller_id,
    seller_zip_code_prefix,
    seller_city,
    seller_state,
    CURRENT_TIMESTAMP() AS last_extract_ts
FROM `dsai-brazilian-ecommerce`.`Brazilian_Ecommerce`.`olist_sellers_dataset`
WHERE 
	seller_id IS NOT NULL 
	AND seller_zip_code_prefix IS NOT NULL
	AND seller_city IS NOT NULL
	AND seller_state IS NOT NULL 
	AND seller_id <> '' 
	AND seller_city <> ''
	AND seller_state <> ''