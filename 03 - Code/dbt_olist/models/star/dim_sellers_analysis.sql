-- models/star/dim_sellers.sql
SELECT
    {{ dbt_utils.surrogate_key(['seller_id']) }} AS seller_sk,
    seller_id,
    seller_zip_code_prefix,
    TRIM(INITCAP(seller_city)) AS seller_city,
    seller_state
FROM {{ ref('stg_olist_sellers') }}
WHERE seller_id IS NOT NULL