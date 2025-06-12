

with source_stg as (
    SELECT *
    FROM `sctp-olist`.`dbt_olist_stg`.`dim_products_stg`
    
    
        where last_extract_ts > (SELECT max(last_extract_ts) FROM `sctp-olist`.`dbt_olist`.`dim_products`)
    
)

SELECT
    S.product_id,
    S.product_category_name,
    S.product_name_lenght,
    S.product_description_lenght,
    S.product_photos_qty,
    S.product_weight_g,
    S.product_length_cm,
    S.product_height_cm,
    S.product_width_cm
FROM source_stg S