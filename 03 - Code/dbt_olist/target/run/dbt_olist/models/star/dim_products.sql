-- back compat for old kwarg name
  
  
        
            
	    
	    
            
        
    

    

    merge into `sctp-olist`.`dbt_olist`.`dim_products` as DBT_INTERNAL_DEST
        using (
        select
        * from `sctp-olist`.`dbt_olist`.`dim_products__dbt_tmp`
        ) as DBT_INTERNAL_SOURCE
        on ((DBT_INTERNAL_SOURCE.product_id = DBT_INTERNAL_DEST.product_id))

    
    when matched then update set
        `product_id` = DBT_INTERNAL_SOURCE.`product_id`,`product_category_name` = DBT_INTERNAL_SOURCE.`product_category_name`,`product_name_lenght` = DBT_INTERNAL_SOURCE.`product_name_lenght`,`product_description_lenght` = DBT_INTERNAL_SOURCE.`product_description_lenght`,`product_photos_qty` = DBT_INTERNAL_SOURCE.`product_photos_qty`,`product_weight_g` = DBT_INTERNAL_SOURCE.`product_weight_g`,`product_length_cm` = DBT_INTERNAL_SOURCE.`product_length_cm`,`product_height_cm` = DBT_INTERNAL_SOURCE.`product_height_cm`,`product_width_cm` = DBT_INTERNAL_SOURCE.`product_width_cm`
    

    when not matched then insert
        (`product_id`, `product_category_name`, `product_name_lenght`, `product_description_lenght`, `product_photos_qty`, `product_weight_g`, `product_length_cm`, `product_height_cm`, `product_width_cm`)
    values
        (`product_id`, `product_category_name`, `product_name_lenght`, `product_description_lenght`, `product_photos_qty`, `product_weight_g`, `product_length_cm`, `product_height_cm`, `product_width_cm`)


    