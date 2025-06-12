SELECT 
  order_id,
  customer_id,
  SAFE_CAST(order_status AS STRING) order_status,
  SAFE_CAST(order_purchase_timestamp AS DATETIME) order_purchase_timestamp,
  SAFE_CAST(order_approved_at AS DATETIME) order_approved_at,
  SAFE_CAST(order_delivered_carrier_date AS DATETIME) order_delivered_carrier_date,
  SAFE_CAST(order_delivered_customer_date AS DATETIME) order_delivered_customer_date,
  SAFE_CAST(order_estimated_delivery_date AS DATETIME) order_estimated_delivery_date,
  CURRENT_TIMESTAMP() AS last_extract_ts
FROM {{ source('olist_raw', 'olist_orders') }} 
WHERE 
	order_id IS NOT NULL
	AND customer_id IS NOT NULL
   	AND order_status IS NOT NULL
   	AND order_purchase_timestamp IS NOT NULL
   	AND order_approved_at IS NOT NULL
   	AND order_delivered_carrier_date IS NOT NULL 
   	AND order_delivered_customer_date IS NOT NULL 
   	AND order_estimated_delivery_date IS NOT NULL
   	AND order_id <> ''
	AND customer_id <> ''
   	AND order_status <> ''