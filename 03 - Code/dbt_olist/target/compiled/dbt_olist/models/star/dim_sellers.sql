

with source_stg as (
    SELECT *
    FROM `sctp-olist`.`dbt_olist_stg`.`dim_sellers_stg`
    
    
        where last_extract_ts > (SELECT max(last_extract_ts) FROM `sctp-olist`.`dbt_olist`.`dim_sellers`)
    
)

SELECT
    S.seller_id,
    S.seller_zip_code_prefix,
    S.seller_city,
    S.seller_state
FROM source_stg S