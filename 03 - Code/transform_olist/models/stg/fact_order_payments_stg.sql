SELECT
    order_id,
    SAFE_CAST(payment_sequential AS NUMERIC) payment_sequential,
    SAFE_CAST(payment_type AS STRING) payment_type,
    SAFE_CAST(payment_installments AS NUMERIC) payment_installments,
    SAFE_CAST(payment_value AS FLOAT64) payment_value,
    CURRENT_TIMESTAMP() AS last_extract_ts
FROM {{ source('olist_raw', 'olist_order_payments') }}
WHERE 
	order_id IS NOT NULL
	AND payment_sequential IS NOT NULL
   	AND payment_type IS NOT NULL
   	AND payment_installments IS NOT NULL
   	AND payment_value IS NOT NULL
   	AND order_id <> '' 
   	AND payment_type <> ''
