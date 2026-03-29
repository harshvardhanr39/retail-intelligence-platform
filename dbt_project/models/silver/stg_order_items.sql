WITH source AS (
    SELECT * FROM {{ source('source_data', 'order_items') }}
),

deduped AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY item_id
            ORDER BY created_at DESC
        ) AS rn
    FROM source
)

SELECT
    item_id::VARCHAR            AS item_id,
    order_id::VARCHAR           AS order_id,
    product_id::VARCHAR         AS product_id,
    quantity::INTEGER           AS quantity,
    unit_price::NUMERIC(10,2)   AS unit_price,
    line_total::NUMERIC(12,2)   AS line_total,
    created_at AT TIME ZONE 'UTC' AS created_at,
    CURRENT_TIMESTAMP           AS _dbt_run_at
FROM deduped
WHERE rn = 1
  AND quantity > 0
  AND unit_price > 0
