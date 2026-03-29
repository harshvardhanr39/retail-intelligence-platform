WITH sales AS (
    SELECT
        oi.product_id,
        COUNT(DISTINCT oi.order_id)     AS order_count,
        SUM(oi.quantity)                AS units_sold,
        ROUND(SUM(oi.line_total), 2)    AS total_revenue,
        ROUND(AVG(oi.unit_price), 2)    AS avg_selling_price
    FROM {{ ref('stg_order_items') }} oi
    JOIN {{ ref('stg_orders') }} o
        ON oi.order_id = o.order_id
    WHERE NOT o.is_cancelled
    GROUP BY oi.product_id
),

ranked AS (
    SELECT
        s.*,
        p.title         AS product_name,
        p.category,
        p.price         AS list_price,
        RANK() OVER (ORDER BY s.total_revenue DESC)     AS revenue_rank,
        RANK() OVER (ORDER BY s.units_sold DESC)        AS units_rank
    FROM sales s
    LEFT JOIN {{ ref('stg_products') }} p
        ON s.product_id = p.product_id
)

SELECT
    revenue_rank        AS rank,
    product_id,
    product_name,
    category,
    order_count,
    units_sold,
    total_revenue,
    avg_selling_price,
    list_price,
    units_rank,
    CURRENT_TIMESTAMP   AS _dbt_run_at
FROM ranked
ORDER BY revenue_rank ASC
