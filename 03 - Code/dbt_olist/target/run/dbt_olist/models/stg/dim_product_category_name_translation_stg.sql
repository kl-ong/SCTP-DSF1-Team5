
  
    

    create or replace table `dsai-brazilian-ecommerce`.`dbt_olist_stg`.`dim_product_category_name_translation_stg`
      
    
    

    OPTIONS()
    as (
      

SELECT
	CAST(string_field_0 AS STRING) AS product_category_name,
	CAST(string_field_1 AS STRING) AS product_category_name_english,
    CURRENT_TIMESTAMP() AS last_extract_ts
FROM `dsai-brazilian-ecommerce`.`Brazilian_Ecommerce`.`product_category_name_translation` 
WHERE 
	string_field_0 IS NOT NULL
	AND string_field_1 IS NOT NULL
	AND string_field_0 <> ''
	AND string_field_1 <> ''
    );
  