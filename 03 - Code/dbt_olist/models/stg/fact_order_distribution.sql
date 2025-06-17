--- This SQL query calculates the number of sellers in each bucket based on the number of unique orders they have received
WITH SellerOrderCounts AS (
    SELECT
        foi.seller_id,
        COUNT(DISTINCT foi.order_id) AS order_count -- Calculate number of unique orders for each seller
    FROM
        {{ ref('fact_order_items') }} AS foi
    JOIN
        {{ ref('dim_orders') }} AS do ON foi.order_id = do.order_id
    WHERE
        do.order_status = 'delivered' -- Filter for delivered orders
        AND do.order_purchase_timestamp BETWEEN '2017-10-01' AND '2018-09-30' -- Filter by purchase date range
    GROUP BY
        foi.seller_id
),
OrderMinMax AS (
    SELECT
        MIN(order_count) AS min_overall_orders,
        MAX(order_count) AS max_overall_orders
    FROM
        SellerOrderCounts
),
BucketedSellersWithCalculatedRange AS (
    SELECT
        soc.seller_id,
        soc.order_count,
        CASE
            -- Handle the edge case where min_overall_orders and max_overall_orders are the same (all sellers have same order count)
            WHEN omm.max_overall_orders = omm.min_overall_orders THEN 1
            -- Calculate the bucket number based on equal order count ranges
            ELSE LEAST(
                    20, -- Cap the bucket number at 20
                    FLOOR((soc.order_count - omm.min_overall_orders) / ((omm.max_overall_orders - omm.min_overall_orders) / 20)) + 1
                 )
        END AS orders_bucket
    FROM
        SellerOrderCounts AS soc, OrderMinMax AS omm
),
BucketCounts AS (
    SELECT
        orders_bucket,
        MIN(order_count) AS min_orders_in_bucket,
        MAX(order_count) AS max_orders_in_bucket,
        COUNT(seller_id) AS number_of_sellers_in_bucket
    FROM
        BucketedSellersWithCalculatedRange
    GROUP BY
        orders_bucket
),
AllBuckets AS (
    -- Generate all 20 bucket numbers
    SELECT CAST(ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS INTEGER) AS bucket_number
    FROM (SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9 UNION ALL SELECT 10 UNION ALL SELECT 11 UNION ALL SELECT 12 UNION ALL SELECT 13 UNION ALL SELECT 14 UNION ALL SELECT 15 UNION ALL SELECT 16 UNION ALL SELECT 17 UNION ALL SELECT 18 UNION ALL SELECT 19 UNION ALL SELECT 20) AS TwentyNumbers
),
FinalBuckets AS (
    SELECT
        ab.bucket_number AS orders_bucket,
        omm.min_overall_orders + ((ab.bucket_number - 1) * ((omm.max_overall_orders - omm.min_overall_orders) / 20)) AS calculated_min_range,
        omm.min_overall_orders + (ab.bucket_number * ((omm.max_overall_orders - omm.min_overall_orders) / 20)) AS calculated_max_range
    FROM
        AllBuckets AS ab, OrderMinMax AS omm
)

SELECT
    fb.orders_bucket,
    fb.calculated_min_range AS min_orders_in_bucket,
    fb.calculated_max_range AS max_orders_in_bucket,
    COALESCE(bc.number_of_sellers_in_bucket, 0) AS number_of_sellers_in_bucket
FROM
    FinalBuckets AS fb
LEFT JOIN
    BucketCounts AS bc ON fb.orders_bucket = bc.orders_bucket
ORDER BY
    fb.orders_bucket ASC
