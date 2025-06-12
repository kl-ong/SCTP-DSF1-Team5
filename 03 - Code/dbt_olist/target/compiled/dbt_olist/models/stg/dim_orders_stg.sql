

SELECT 
  order_id,
  customer_id,
  order_status,
  order_purchase_timestamp,
  order_approved_at,
  order_delivered_carrier_date,
  order_delivered_customer_date,
  order_estimated_delivery_date,
  CURRENT_TIMESTAMP() AS last_extract_ts
FROM `dsai-brazilian-ecommerce`.`Brazilian_Ecommerce`.`olist_orders_dataset` 
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