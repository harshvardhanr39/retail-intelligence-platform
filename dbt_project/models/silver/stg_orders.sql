WITH source AS (
    SELECT * FROM {{ source('source_data', 'orders') }}
),

deduped AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY order_id
            ORDER BY updated_at DESC
        ) AS rn
    FROM source
),

cleaned AS (
    SELECT
        order_id::VARCHAR                                       AS order_id,
        customer_id::VARCHAR                                    AS customer_id,
        status,
        currency,
        COALESCE(subtotal, 0)::NUMERIC(12,2)                   AS subtotal,
        COALESCE(discount_amount, 0)::NUMERIC(12,2)            AS discount_amount,
        COALESCE(tax_amount, 0)::NUMERIC(12,2)                 AS tax_amount,
        COALESCE(shipping_amount, 0)::NUMERIC(12,2)            AS shipping_amount,
        total_amount::NUMERIC(12,2)                            AS total_amount,
        created_at AT TIME ZONE 'UTC'                          AS created_at,
        updated_at AT TIME ZONE 'UTC'                          AS updated_at,
        DATE(created_at AT TIME ZONE 'UTC')                    AS order_date,
        CASE WHEN status = 'delivered' THEN TRUE ELSE FALSE END AS is_completed,
        CASE WHEN status = 'cancelled' THEN TRUE ELSE FALSE END AS is_cancelled,
        CURRENT_TIMESTAMP                                       AS _dbt_run_at
    FROM deduped
    WHERE rn = 1
      AND total_amount IS NOT NULL
      AND total_amount > 0
)

SELECT * FROM cleaned
