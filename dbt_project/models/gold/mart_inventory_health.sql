WITH inventory AS (
    SELECT * FROM {{ ref('stg_inventory') }}
),

avg_daily_sales AS (
    SELECT
        oi.product_id,
        ROUND(
            SUM(oi.quantity)::NUMERIC /
            NULLIF(COUNT(DISTINCT o.order_date), 0)
        , 2)                        AS avg_daily_units_sold
    FROM {{ ref('stg_order_items') }} oi
    JOIN {{ ref('stg_orders') }} o
        ON oi.order_id = o.order_id
    WHERE o.order_date >= CURRENT_DATE - INTERVAL '30 days'
      AND NOT o.is_cancelled
    GROUP BY oi.product_id
)

SELECT
    i.sku_id,
    i.product_name,
    i.product_id,
    i.warehouse_id,
    i.quantity_on_hand,
    i.reorder_point,
    i.supplier_code,
    i.last_count_date,
    COALESCE(s.avg_daily_units_sold, 0)     AS avg_daily_units_sold,
    CASE
        WHEN COALESCE(s.avg_daily_units_sold, 0) = 0 THEN NULL
        ELSE ROUND(
            i.quantity_on_hand::NUMERIC /
            s.avg_daily_units_sold
        , 1)
    END                                     AS days_of_stock,
    CASE
        WHEN i.quantity_on_hand = 0                         THEN 'out_of_stock'
        WHEN i.quantity_on_hand < i.reorder_point           THEN 'low_stock'
        WHEN i.quantity_on_hand > i.reorder_point * 3       THEN 'overstock'
        ELSE 'healthy'
    END                                     AS status_flag,
    i.had_negative_quantity,
    CURRENT_TIMESTAMP                       AS _dbt_run_at
FROM inventory i
LEFT JOIN avg_daily_sales s ON i.product_id = s.product_id
ORDER BY days_of_stock ASC NULLS FIRST
