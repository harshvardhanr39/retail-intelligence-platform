WITH daily AS (
    SELECT
        order_date                                          AS date,
        COUNT(DISTINCT order_id)                            AS order_count,
        SUM(total_amount)                                   AS total_revenue,
        ROUND(AVG(total_amount), 2)                         AS avg_order_value,
        COUNT(CASE WHEN is_cancelled THEN 1 END)            AS cancelled_orders,
        COUNT(CASE WHEN is_completed THEN 1 END)            AS completed_orders,
        SUM(CASE WHEN is_completed THEN total_amount
            ELSE 0 END)                                     AS completed_revenue
    FROM {{ ref('stg_orders') }}
    GROUP BY order_date
),

with_rolling AS (
    SELECT
        *,
        ROUND(
            AVG(total_revenue) OVER (
                ORDER BY date
                ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
            ), 2
        )                                                   AS revenue_7day_avg,
        LAG(total_revenue) OVER (ORDER BY date)             AS prev_day_revenue
    FROM daily
)

SELECT
    date,
    order_count,
    ROUND(total_revenue, 2)                                 AS total_revenue,
    avg_order_value,
    cancelled_orders,
    completed_orders,
    ROUND(completed_revenue, 2)                             AS completed_revenue,
    revenue_7day_avg,
    ROUND(prev_day_revenue, 2)                              AS prev_day_revenue,
    CASE
        WHEN prev_day_revenue IS NULL OR prev_day_revenue = 0 THEN NULL
        ELSE ROUND(
            ((total_revenue - prev_day_revenue) / prev_day_revenue) * 100
        , 2)
    END                                                     AS revenue_growth_pct,
    CURRENT_TIMESTAMP                                       AS _dbt_run_at
FROM with_rolling
ORDER BY date DESC
