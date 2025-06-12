

with source_stg as (
    SELECT *
    FROM `sctp-olist`.`dbt_olist_stg`.`fact_order_items_stg`
    
    
        where last_extract_ts > (SELECT max(last_extract_ts) FROM `sctp-olist`.`dbt_olist`.`fact_order_items`)
    
)

SELECT
    S.order_id,
    S.order_item_id,
    S.product_id,
    S.seller_id,
    S.shipping_limit_date,
    S.price,
    S.freight_value
FROM source_stg S