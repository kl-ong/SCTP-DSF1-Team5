

with source_stg as (
    SELECT *
    FROM `sctp-olist`.`dbt_olist_stg`.`fact_order_payments_stg`
    
    
        where last_extract_ts > (SELECT max(last_extract_ts) FROM `sctp-olist`.`dbt_olist`.`fact_order_payments`)
    
)

SELECT
    S.order_id,
    S.payment_sequential,
    S.payment_type,
    S.payment_installments,
    S.payment_value
FROM source_stg S