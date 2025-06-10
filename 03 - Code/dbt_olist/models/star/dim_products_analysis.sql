
-- models/star/dim_products.sql
SELECT
    {{ dbt_utils.surrogate_key(['p.product_id']) }} AS product_sk,
    p.product_id,
    COALESCE(t.product_category_name_english, p.product_category_name) AS product_category_name_english,
    p.product_category_name,
    p.product_name_length,
    p.product_description_length,
    p.product_photos_qty,
    CASE WHEN p.product_weight_g > 0 THEN p.product_weight_g ELSE NULL END AS product_weight_g,
    CASE WHEN p.product_length_cm > 0 THEN p.product_length_cm ELSE NULL END AS product_length_cm,
    CASE WHEN p.product_height_cm > 0 THEN p.product_height_cm ELSE NULL END AS product_height_cm,
    CASE WHEN p.product_width_cm > 0 THEN p.product_width_cm ELSE NULL END AS product_width_cm
FROM {{ ref('stg_olist_products') }} p
LEFT JOIN {{ ref('stg_olist_product_category_translation') }} t
    ON p.product_category_name = t.product_category_name
WHERE p.product_id IS NOT NULL
  AND p.product_category_name IS NOT NULL 