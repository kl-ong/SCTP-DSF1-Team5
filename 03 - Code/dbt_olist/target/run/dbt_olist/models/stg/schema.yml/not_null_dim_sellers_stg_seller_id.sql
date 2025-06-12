
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select seller_id
from `sctp-olist`.`dbt_olist_stg`.`dim_sellers_stg`
where seller_id is null



  
  
      
    ) dbt_internal_test