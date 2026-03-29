WITH source AS (
    SELECT * FROM {{ source('source_data', 'inventory') }}
),

deduped AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY sku_id
            ORDER BY last_count_date DESC NULLS LAST
        ) AS rn
    FROM source
)

SELECT
    sku_id,
    product_name,
    product_id::VARCHAR                             AS product_id,
    warehouse_id,
    -- coerce negative quantities to 0 (data quality fix)
    GREATEST(COALESCE(quantity_on_hand, 0), 0)      AS quantity_on_hand,
    COALESCE(reorder_point, 0)                      AS reorder_point,
    last_count_date,
    supplier_code,
    CASE
        WHEN quantity_on_hand < 0 THEN TRUE
        ELSE FALSE
    END                                             AS had_negative_quantity,
    CURRENT_TIMESTAMP                               AS _dbt_run_at
FROM deduped
WHERE rn = 1
