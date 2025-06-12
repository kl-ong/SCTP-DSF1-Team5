-- back compat for old kwarg name
  
  
        
            
	    
	    
            
        
    

    

    merge into `sctp-olist`.`dbt_olist`.`dim_sellers` as DBT_INTERNAL_DEST
        using (
        select
        * from `sctp-olist`.`dbt_olist`.`dim_sellers__dbt_tmp`
        ) as DBT_INTERNAL_SOURCE
        on ((DBT_INTERNAL_SOURCE.seller_id = DBT_INTERNAL_DEST.seller_id))

    
    when matched then update set
        `seller_id` = DBT_INTERNAL_SOURCE.`seller_id`,`seller_zip_code_prefix` = DBT_INTERNAL_SOURCE.`seller_zip_code_prefix`,`seller_city` = DBT_INTERNAL_SOURCE.`seller_city`,`seller_state` = DBT_INTERNAL_SOURCE.`seller_state`
    

    when not matched then insert
        (`seller_id`, `seller_zip_code_prefix`, `seller_city`, `seller_state`)
    values
        (`seller_id`, `seller_zip_code_prefix`, `seller_city`, `seller_state`)


    