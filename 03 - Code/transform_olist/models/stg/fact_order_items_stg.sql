SELECT
    order_id,
    order_item_id,
    product_id,
    seller_id,
    SAFE_CAST(shipping_limit_date AS DATETIME) shipping_limit_date,
    SAFE_CAST(price AS FLOAT64) price,
    SAFE_CAST(freight_value AS FLOAT64) freight_value,
    CURRENT_TIMESTAMP() AS last_extract_ts
FROM {{ source('olist_raw', 'olist_order_items') }}
WHERE 
	order_id IS NOT NULL
	AND order_item_id IS NOT NULL
   	AND product_id IS NOT NULL
   	AND seller_id IS NOT NULL
   	AND shipping_limit_date IS NOT NULL
   	AND price IS NOT NULL 
   	AND freight_Value IS NOT NULL
   	AND order_id <> ''
	AND product_id <> ''
	AND seller_id <> ''