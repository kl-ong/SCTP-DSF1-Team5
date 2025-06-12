
  
    

    create or replace table `dsai-brazilian-ecommerce`.`dbt_olist`.`fact_order_items_analysis`
      
    
    

    OPTIONS()
    as (
      


-- models/star/fact_order_items.sql
-- This model creates the fact_order_items table, centralizing order item details,
-- linking to dimensions, and calculating key metrics.

SELECT
    o.order_id,
    o.customer_id,
    s.seller_id, 
    t.product_category_name_english,
    oi.order_item_id,
    EXTRACT(DAY FROM (o.order_delivered_customer_date - o.order_purchase_timestamp)) AS final_order_fullfilment_days,
    o.order_status,
    oi.price,
    oi.freight_value,
    (oi.price + oi.freight_value) AS total_sale_amount, 
    FIRST_VALUE(op.payment_type) OVER (PARTITION BY o.order_id ORDER BY op.payment_type) AS payment_type,
    FIRST_VALUE(op.payment_installments) OVER (PARTITION BY o.order_id ORDER BY op.payment_type) AS payment_installments,
    FIRST_VALUE(op.payment_value) OVER (PARTITION BY o.order_id ORDER BY op.payment_type) AS payment_value

FROM `dsai-brazilian-ecommerce`.`dbt_olist`.`dim_orders_analysis` AS o

	JOIN `dsai-brazilian-ecommerce`.`dbt_olist_stg`.`fact_order_items_stg` AS oi
	    ON o.order_id = oi.order_id
	LEFT JOIN `dsai-brazilian-ecommerce`.`dbt_olist`.`fact_order_payments_analysis` AS op
	    ON o.order_id = op.order_id
	LEFT JOIN `dsai-brazilian-ecommerce`.`dbt_olist`.`dim_products_analysis` AS p
	    ON oi.product_id = p.product_id
	LEFT JOIN `dsai-brazilian-ecommerce`.`dbt_olist_stg`.`dim_product_category_name_translation_stg` AS t
    	ON p.product_category_name = t.product_category_name
	LEFT JOIN `dsai-brazilian-ecommerce`.`dbt_olist`.`dim_sellers_analysis` AS s
	    ON oi.seller_id = s.seller_id
WHERE 
	o.order_id IS NOT NULL 
	AND 
	oi.order_item_id IS NOT NULL
LIMIT 1000
    );
  