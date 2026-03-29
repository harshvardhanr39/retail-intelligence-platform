WITH source AS (
    SELECT * FROM {{ source('source_data', 'products') }}
),

deduped AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY product_id
            ORDER BY created_at DESC
        ) AS rn
    FROM source
)

SELECT
    product_id::VARCHAR         AS product_id,
    title,
    category,
    price::NUMERIC(10,2)        AS price,
    description,
    image_url,
    COALESCE(rating, 0)::NUMERIC(3,2)   AS rating,
    COALESCE(rating_count, 0)::INTEGER  AS rating_count,
    created_at AT TIME ZONE 'UTC'       AS created_at,
    CURRENT_TIMESTAMP                   AS _dbt_run_at
FROM deduped
WHERE rn = 1
  AND price IS NOT NULL
  AND price > 0
