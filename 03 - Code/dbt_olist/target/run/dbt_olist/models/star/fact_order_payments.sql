-- back compat for old kwarg name
  
  
        
            
                
                
            
                
                
            
        
    

    

    merge into `sctp-olist`.`dbt_olist`.`fact_order_payments` as DBT_INTERNAL_DEST
        using (
        select
        * from `sctp-olist`.`dbt_olist`.`fact_order_payments__dbt_tmp`
        ) as DBT_INTERNAL_SOURCE
        on (
                    DBT_INTERNAL_SOURCE.order_id = DBT_INTERNAL_DEST.order_id
                ) and (
                    DBT_INTERNAL_SOURCE.payment_sequential = DBT_INTERNAL_DEST.payment_sequential
                )

    
    when matched then update set
        `order_id` = DBT_INTERNAL_SOURCE.`order_id`,`payment_sequential` = DBT_INTERNAL_SOURCE.`payment_sequential`,`payment_type` = DBT_INTERNAL_SOURCE.`payment_type`,`payment_installments` = DBT_INTERNAL_SOURCE.`payment_installments`,`payment_value` = DBT_INTERNAL_SOURCE.`payment_value`
    

    when not matched then insert
        (`order_id`, `payment_sequential`, `payment_type`, `payment_installments`, `payment_value`)
    values
        (`order_id`, `payment_sequential`, `payment_type`, `payment_installments`, `payment_value`)


    