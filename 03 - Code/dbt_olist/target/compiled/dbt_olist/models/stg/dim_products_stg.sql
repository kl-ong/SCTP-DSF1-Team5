

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
FROM `dsai-brazilian-ecommerce`.`Brazilian_Ecommerce`.`olist_products_dataset`
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