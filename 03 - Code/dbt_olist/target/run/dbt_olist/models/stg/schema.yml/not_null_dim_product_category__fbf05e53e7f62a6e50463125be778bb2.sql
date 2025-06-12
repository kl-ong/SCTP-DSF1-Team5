
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select product_category_name_english
from `sctp-olist`.`dbt_olist_stg`.`dim_product_category_name_translation_stg`
where product_category_name_english is null



  
  
      
    ) dbt_internal_test