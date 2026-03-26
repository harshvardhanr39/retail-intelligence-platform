-- ── CREATE SCHEMAS 
CREATE SCHEMA IF NOT EXISTS source_data;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;
CREATE SCHEMA IF NOT EXISTS data_quality;
CREATE SCHEMA IF NOT EXISTS pipeline_meta;


-- ── SOURCE TABLES (transactional) ────────────────────────────────
CREATE TABLE IF NOT EXISTS source_data.customers (
    customer_id     UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    email           VARCHAR(255) UNIQUE NOT NULL,
    first_name      VARCHAR(100) NOT NULL,
    last_name       VARCHAR(100) NOT NULL,
    city            VARCHAR(100),
    country         VARCHAR(100),
    segment         VARCHAR(50),
    created_at      TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at      TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS source_data.products (
    product_id      VARCHAR(50) PRIMARY KEY,
    title           VARCHAR(500) NOT NULL,
    category        VARCHAR(100),
    price           DECIMAL(10,2),
    description     TEXT,
    image_url       VARCHAR(500),
    rating          DECIMAL(3,2),
    rating_count    INTEGER,
    created_at      TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at      TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS source_data.orders (
    order_id        UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    customer_id     UUID REFERENCES source_data.customers(customer_id),
    status          VARCHAR(50) NOT NULL DEFAULT 'pending',
    currency        VARCHAR(10) DEFAULT 'USD',
    subtotal        DECIMAL(12,2),
    discount_amount DECIMAL(12,2) DEFAULT 0,
    tax_amount      DECIMAL(12,2) DEFAULT 0,
    shipping_amount DECIMAL(12,2) DEFAULT 0,
    total_amount    DECIMAL(12,2),
    created_at      TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at      TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);


CREATE TABLE IF NOT EXISTS source_data.order_items (
    item_id         UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    order_id        UUID REFERENCES source_data.orders(order_id),
    product_id      VARCHAR(50),
    quantity        INTEGER NOT NULL CHECK (quantity > 0),
    unit_price      DECIMAL(10,2) NOT NULL,
    line_total      DECIMAL(12,2) GENERATED ALWAYS AS (quantity * unit_price) STORED,
    created_at      TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- ── PIPELINE METADATA TABLES ──────────────────────────────────────
CREATE TABLE IF NOT EXISTS pipeline_meta.pipeline_runs (
    run_id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    source_name     VARCHAR(100) NOT NULL,
    dag_id          VARCHAR(200),
    run_date        DATE NOT NULL,
    started_at      TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    finished_at     TIMESTAMP WITH TIME ZONE,
    status          VARCHAR(20) DEFAULT 'running',
    rows_extracted  INTEGER DEFAULT 0,
    rows_written    INTEGER DEFAULT 0,
    error_message   TEXT,
    watermark_from  TIMESTAMP WITH TIME ZONE,
    watermark_to    TIMESTAMP WITH TIME ZONE
);

CREATE TABLE IF NOT EXISTS pipeline_meta.watermarks (
    source_name     VARCHAR(100) PRIMARY KEY,
    last_run_at     TIMESTAMP WITH TIME ZONE NOT NULL,
    last_watermark  TIMESTAMP WITH TIME ZONE NOT NULL,
    updated_at      TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- ── DATA QUALITY RESULTS ──────────────────────────────────────────
CREATE TABLE IF NOT EXISTS data_quality.quality_results (
    result_id       UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    run_id          UUID,
    check_name      VARCHAR(200) NOT NULL,
    table_name      VARCHAR(200) NOT NULL,
    column_name     VARCHAR(200),
    result          VARCHAR(20) NOT NULL,  -- pass / warn / fail
    failing_rows    INTEGER DEFAULT 0,
    total_rows      INTEGER,
    details         TEXT,
    checked_at      TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- ── INDEXES ───────────────────────────────────────────────────────
CREATE INDEX IF NOT EXISTS idx_orders_updated ON source_data.orders(updated_at);
CREATE INDEX IF NOT EXISTS idx_orders_customer ON source_data.orders(customer_id);
CREATE INDEX IF NOT EXISTS idx_order_items_order ON source_data.order_items(order_id);
CREATE INDEX IF NOT EXISTS idx_pipeline_runs_source ON pipeline_meta.pipeline_runs(source_name, run_date);

-- ── ENABLE LOGICAL REPLICATION (for Debezium CDC) ─────────────────
ALTER TABLE source_data.orders REPLICA IDENTITY FULL;
ALTER TABLE source_data.order_items REPLICA IDENTITY FULL;
ALTER TABLE source_data.customers REPLICA IDENTITY FULL;

-- ── PUBLICATION (for Debezium) ─────────────────────────────────────
CREATE PUBLICATION retail_cdc FOR TABLE
    source_data.orders,
    source_data.order_items,
    source_data.customers;