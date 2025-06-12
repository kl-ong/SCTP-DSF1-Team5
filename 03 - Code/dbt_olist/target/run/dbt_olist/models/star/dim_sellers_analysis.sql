
  
    

    create or replace table `dsai-brazilian-ecommerce`.`dbt_olist`.`dim_sellers_analysis`
      
    
    

    OPTIONS()
    as (
      

-- models/star/dim_sellers.sql
SELECT
    s.seller_id,
    oi.order_id,    
    s.seller_zip_code_prefix,
    TRIM(INITCAP(s.seller_city)) AS seller_city,
    s.seller_state
FROM `dsai-brazilian-ecommerce`.`dbt_olist_stg`.`dim_sellers_stg` s
LEFT JOIN `dsai-brazilian-ecommerce`.`dbt_olist_stg`.`fact_order_items_stg` oi ON s.seller_id = oi.seller_id
WHERE 
	s.seller_id IS NOT NULL
    );
  