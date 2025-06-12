

  create or replace view `sctp-olist`.`dbt_olist`.`dim_order_items`
  OPTIONS()
  as SELECT
    order_id,
    order_item_id,
    product_id,
    seller_id,
    shipping_limit_date,
    price,
    freight_value
FROM `dsai-brazilian-ecommerce`.`Brazilian_Ecommerce`.`olist_order_items_dataset`;

