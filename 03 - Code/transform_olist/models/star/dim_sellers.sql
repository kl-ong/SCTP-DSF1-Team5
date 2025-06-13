
SELECT DISTINCT
    s.seller_id,
    oi.order_id,    
    s.seller_zip_code_prefix,
    TRIM(INITCAP(s.seller_city)) AS seller_city,
    s.seller_state
FROM {{ ref('dim_sellers_stg') }} s
LEFT JOIN {{ ref('fact_order_items_stg') }} oi ON s.seller_id = oi.seller_id
WHERE 
	s.seller_id IS NOT NULL
