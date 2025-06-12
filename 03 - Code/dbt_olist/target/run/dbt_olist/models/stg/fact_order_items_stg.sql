
  
    

    create or replace table `dsai-brazilian-ecommerce`.`dbt_olist_stg`.`fact_order_items_stg`
      
    
    

    OPTIONS()
    as (
      

SELECT
    order_id,
    order_item_id,
    product_id,
    seller_id,
    shipping_limit_date,
    price,
    freight_value,
    CURRENT_TIMESTAMP() AS last_extract_ts
FROM `dsai-brazilian-ecommerce`.`Brazilian_Ecommerce`.`olist_order_items_dataset`
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
LIMIT 1000
    );
  