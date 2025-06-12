
  
    

    create or replace table `sctp-olist`.`dbt_olist_stg`.`dim_translate_stg`
      
    
    

    OPTIONS()
    as (
      

SELECT
    seller_id,
    seller_zip_code_prefix,
    seller_city,
    seller_state,
    CURRENT_TIMESTAMP() AS last_extract_ts
FROM `dsai-brazilian-ecommerce`.`Brazilian_Ecommerce`.`olist_sellers_dataset`
WHERE 
	seller_id IS NULL 
	AND seller_zip_code_prefix IS NULL
	AND seller_city IS NULL
	AND seller_state IS NULL 
	AND seller_id = '' 
	AND seller_city = ''
	AND seller_state = ''
    );
  