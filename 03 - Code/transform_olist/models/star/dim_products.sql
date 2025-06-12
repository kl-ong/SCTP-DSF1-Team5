SELECT
    p.product_id,
    t.product_category_name_english,
    p.product_category_name,
    p.product_name_lenght,
    p.product_description_lenght,
    p.product_photos_qty,
    CASE WHEN p.product_weight_g > 0 THEN p.product_weight_g ELSE NULL END AS product_weight_g,
    CASE WHEN p.product_length_cm > 0 THEN p.product_length_cm ELSE NULL END AS product_length_cm,
    CASE WHEN p.product_height_cm > 0 THEN p.product_height_cm ELSE NULL END AS product_height_cm,
    CASE WHEN p.product_width_cm > 0 THEN p.product_width_cm ELSE NULL END AS product_width_cm
FROM {{ ref('dim_products_stg') }} p
LEFT JOIN {{ ref('dim_product_category_name_translation_stg') }} t
    ON p.product_category_name = t.product_category_name
WHERE p.product_id IS NOT NULL
  AND p.product_category_name IS NOT NULL 