WITH order_items_products AS (
    SELECT
        oi.order_id,
        oi.product_id,
        oi.quantity,
        oi.unit_price,
        oi.line_total,
        p.category,
        p.title         AS product_name
    FROM {{ ref('stg_order_items') }} oi
    LEFT JOIN {{ ref('stg_products') }} p
        ON oi.product_id = p.product_id
),

orders_with_date AS (
    SELECT
        o.order_id,
        o.order_date
    FROM {{ ref('stg_orders') }} o
    WHERE NOT o.is_cancelled
),

joined AS (
    SELECT
        od.order_date,
        oip.category,
        oip.quantity,
        oip.line_total
    FROM order_items_products oip
    JOIN orders_with_date od ON oip.order_id = od.order_id
    WHERE oip.category IS NOT NULL
),

aggregated AS (
    SELECT
        order_date,
        category,
        COUNT(*)                    AS order_count,
        SUM(quantity)               AS units_sold,
        ROUND(SUM(line_total), 2)   AS revenue
    FROM joined
    GROUP BY order_date, category
),

with_share AS (
    SELECT
        *,
        ROUND(
            revenue / NULLIF(
                SUM(revenue) OVER (PARTITION BY order_date)
            , 0) * 100
        , 2)                        AS revenue_share_pct
    FROM aggregated
)

SELECT
    order_date          AS date,
    category,
    order_count,
    units_sold,
    revenue,
    revenue_share_pct,
    CURRENT_TIMESTAMP   AS _dbt_run_at
FROM with_share
ORDER BY order_date DESC, revenue DESC
