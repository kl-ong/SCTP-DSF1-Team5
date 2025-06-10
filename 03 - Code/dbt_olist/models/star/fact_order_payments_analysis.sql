-- models/star/fact_order_payments.sql
-- This model focuses on payment transaction details.
-- You might choose to aggregate payments per order here if needed,
-- or keep it granular per payment_sequential.

SELECT
    {{ dbt_utils.surrogate_key(['op.order_id', 'op.payment_sequential']) }} AS payment_sk,
    op.order_id,
    op.payment_sequential,
    op.payment_type,
    op.payment_installments,
    op.payment_value,
    o.order_purchase_timestamp, -- Link to order timestamp for context
    c.customer_sk -- Link to customer dimension
FROM {{ ref('stg_olist_order_payments') }} op
LEFT JOIN {{ ref('stg_olist_orders') }} o
    ON op.order_id = o.order_id
LEFT JOIN {{ ref('dim_customers') }} c
    ON o.customer_id = c.customer_id
WHERE op.order_id IS NOT NULL