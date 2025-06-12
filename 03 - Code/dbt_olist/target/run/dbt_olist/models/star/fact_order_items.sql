-- back compat for old kwarg name
  
  
        
            
                
                
            
                
                
            
        
    

    

    merge into `sctp-olist`.`dbt_olist`.`fact_order_items` as DBT_INTERNAL_DEST
        using (
        select
        * from `sctp-olist`.`dbt_olist`.`fact_order_items__dbt_tmp`
        ) as DBT_INTERNAL_SOURCE
        on (
                    DBT_INTERNAL_SOURCE.order_id = DBT_INTERNAL_DEST.order_id
                ) and (
                    DBT_INTERNAL_SOURCE.order_item_id = DBT_INTERNAL_DEST.order_item_id
                )

    
    when matched then update set
        `order_id` = DBT_INTERNAL_SOURCE.`order_id`,`order_item_id` = DBT_INTERNAL_SOURCE.`order_item_id`,`product_id` = DBT_INTERNAL_SOURCE.`product_id`,`seller_id` = DBT_INTERNAL_SOURCE.`seller_id`,`shipping_limit_date` = DBT_INTERNAL_SOURCE.`shipping_limit_date`,`price` = DBT_INTERNAL_SOURCE.`price`,`freight_value` = DBT_INTERNAL_SOURCE.`freight_value`
    

    when not matched then insert
        (`order_id`, `order_item_id`, `product_id`, `seller_id`, `shipping_limit_date`, `price`, `freight_value`)
    values
        (`order_id`, `order_item_id`, `product_id`, `seller_id`, `shipping_limit_date`, `price`, `freight_value`)


    