
  
    

    create or replace table `dsai-brazilian-ecommerce`.`dbt_olist_stg`.`fact_order_payments_stg`
      
    
    

    OPTIONS()
    as (
      

SELECT
    order_id,
    payment_sequential,
    payment_type,
    payment_installments,
    payment_value,
    CURRENT_TIMESTAMP() AS last_extract_ts
FROM `dsai-brazilian-ecommerce`.`Brazilian_Ecommerce`.`olist_order_payments_dataset`
WHERE 
	order_id IS NOT NULL
	AND payment_sequential IS NOT NULL
   	AND payment_type IS NOT NULL
   	AND payment_installments IS NOT NULL
   	AND payment_value IS NOT NULL
   	AND order_id <> '' 
   	AND payment_type <> ''
LIMIT 1000
    );
  