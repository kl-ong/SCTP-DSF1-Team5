{{ config(materialized='table') }}

with orders_stg as (
    SELECT *
    FROM {{ ref('dim_orders_stg') }}
    
    {% if is_incremental() %}
        where last_extract_ts > (SELECT max(last_extract_ts) FROM {{ this }})
    {% endif %}
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