-- models/star/fact_order_items.sql
-- This model creates the fact_order_items table, centralizing order item details,
-- linking to dimensions, and calculating key metrics.

SELECT
    {{ dbt_utils.surrogate_key(['o.order_id', 'oi.order_item_id']) }} AS order_item_sk,
    o.order_id,
    oi.order_item_id,
    c.customer_sk, -- Foreign Key to dim_customers
    p.product_sk,  -- Foreign Key to dim_products
    s.seller_sk,   -- Foreign Key to dim_sellers

    -- Dates/Timestamps (ensure proper casting to TIMESTAMP or DATE for BigQuery)
    PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', o.order_purchase_timestamp) AS order_purchase_timestamp,
    PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', o.order_approved_at) AS order_approved_at,
    PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', o.order_delivered_carrier_date) AS order_delivered_carrier_date,
    PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', o.order_delivered_customer_date) AS order_delivered_customer_date,
    PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', o.order_estimated_delivery_date) AS order_estimated_delivery_date,

    o.order_status,
    oi.price,
    oi.freight_value,
    (oi.price + oi.freight_value) AS total_sale_amount, -- Derived column 

    -- Payment details (handle cases where there might be multiple payment rows per order)
    -- For simplicity, we'll take the first payment details linked to the order_id for this item.
    -- In a real scenario, you might aggregate payments first or create a separate payment fact.
    FIRST_VALUE(op.payment_type) OVER (PARTITION BY o.order_id ORDER BY op.payment_type) AS payment_type,
    FIRST_VALUE(op.payment_installments) OVER (PARTITION BY o.order_id ORDER BY op.payment_type) AS payment_installments,
    FIRST_VALUE(op.payment_value) OVER (PARTITION BY o.order_id ORDER BY op.payment_type) AS payment_value,

    -- Review score (handle cases where there might be multiple reviews per order)
    -- For simplicity, take the first review score linked to the order_id for this item.
    -- You might average review scores or select the latest one.
    COALESCE(FIRST_VALUE(rev.review_score) OVER (PARTITION BY o.order_id ORDER BY rev.review_creation_date DESC), 0) AS review_score,
    rev.review_comment_message,
    CAST(rev.review_creation_date AS DATE) AS review_creation_date,
    CAST(rev.review_answer_timestamp AS TIMESTAMP) AS review_answer_timestamp

FROM {{ ref('stg_olist_orders') }} o
JOIN {{ ref('stg_olist_order_items') }} oi
    ON o.order_id = oi.order_id
LEFT JOIN {{ ref('stg_olist_order_payments') }} op
    ON o.order_id = op.order_id
LEFT JOIN {{ ref('stg_olist_order_reviews') }} rev
    ON o.order_id = rev.order_id
LEFT JOIN {{ ref('dim_customers') }} c
    ON o.customer_id = c.customer_id
LEFT JOIN {{ ref('dim_products') }} p
    ON oi.product_id = p.product_id
LEFT JOIN {{ ref('dim_sellers') }} s
    ON oi.seller_id = s.seller_id
WHERE o.order_id IS NOT NULL AND oi.order_item_id IS NOT NULL
-- Add more robust filtering, e.g., to exclude canceled orders from analysis
-- AND o.order_status NOT IN ('canceled', 'unavailable')