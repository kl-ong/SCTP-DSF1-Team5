
-- This model focuses on payment transaction details.
-- You might choose to aggregate payments per order here if needed,
-- or keep it granular per payment_sequential.

SELECT DISTINCT
    op.order_id,
    op.payment_sequential,
    op.payment_type,
    op.payment_installments,
    op.payment_value,
    o.customer_id,
    o.order_status,
    o.order_purchase_timestamp,
    o.order_approved_at,
    o.order_delivered_carrier_date,
    o.order_delivered_customer_date,
    o.order_estimated_delivery_date
FROM {{ ref('fact_order_payments_stg') }} AS op
LEFT JOIN {{ ref('dim_orders') }} AS o
    ON op.order_id = o.order_id
WHERE op.order_id IS NOT NULL