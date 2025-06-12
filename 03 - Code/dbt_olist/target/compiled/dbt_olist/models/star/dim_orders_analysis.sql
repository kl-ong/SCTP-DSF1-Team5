

with orders_stg as (
    SELECT *
    FROM `dsai-brazilian-ecommerce`.`dbt_olist_stg`.`dim_orders_stg`
    
    
)

SELECT
    order_id,
    customer_id,
    order_status,
    EXTRACT(DAY FROM (order_delivered_customer_date - order_purchase_timestamp)) AS final_order_fullfilment_days,
    order_purchase_timestamp,
    order_approved_at,
    order_delivered_carrier_date,
    order_delivered_customer_date,
    order_estimated_delivery_date
FROM orders_stg 
WHERE order_id IS NOT NULL