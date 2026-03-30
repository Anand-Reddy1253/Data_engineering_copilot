-- create_staging.sql
-- Staging schema DDL for Contoso Fabric Platform DuckDB warehouse
-- Staging tables are transient and used for data loading

CREATE SCHEMA IF NOT EXISTS staging;

-- ============================================================
-- staging.pos_transactions
-- ============================================================
CREATE TABLE IF NOT EXISTS staging.pos_transactions (
    transaction_id  VARCHAR     NOT NULL,
    store_id        VARCHAR     NOT NULL,
    product_id      VARCHAR     NOT NULL,
    customer_id     VARCHAR,
    quantity        INTEGER     NOT NULL,
    unit_price      DECIMAL(12, 2) NOT NULL,
    sale_date       TIMESTAMP   NOT NULL,
    payment_method  VARCHAR(20) NOT NULL,
    total_amount    DECIMAL(14, 2),
    _ingested_at    TIMESTAMP   NOT NULL DEFAULT CURRENT_TIMESTAMP,
    _source_file    VARCHAR,
    CONSTRAINT pk_staging_pos_transactions PRIMARY KEY (transaction_id)
);

-- ============================================================
-- staging.inventory_movements
-- ============================================================
CREATE TABLE IF NOT EXISTS staging.inventory_movements (
    movement_id     VARCHAR     NOT NULL,
    warehouse_id    VARCHAR     NOT NULL,
    product_id      VARCHAR     NOT NULL,
    movement_type   VARCHAR(3)  NOT NULL CHECK (movement_type IN ('IN', 'OUT')),
    quantity        INTEGER     NOT NULL CHECK (quantity > 0),
    movement_date   TIMESTAMP   NOT NULL,
    _ingested_at    TIMESTAMP   NOT NULL DEFAULT CURRENT_TIMESTAMP,
    _source_file    VARCHAR,
    CONSTRAINT pk_staging_inventory_movements PRIMARY KEY (movement_id)
);

-- ============================================================
-- staging.customers
-- ============================================================
CREATE TABLE IF NOT EXISTS staging.customers (
    customer_id     VARCHAR     NOT NULL,
    name_hash       VARCHAR(64),
    email_hash      VARCHAR(64),
    phone_hash      VARCHAR(64),
    address_hash    VARCHAR(64),
    city            VARCHAR,
    country         VARCHAR,
    signup_date     TIMESTAMP,
    _ingested_at    TIMESTAMP   NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT pk_staging_customers PRIMARY KEY (customer_id)
);

-- ============================================================
-- staging.clickstream
-- ============================================================
CREATE TABLE IF NOT EXISTS staging.clickstream (
    session_id              VARCHAR     NOT NULL,
    customer_id             VARCHAR,
    page_url                VARCHAR     NOT NULL,
    event_type              VARCHAR(50) NOT NULL,
    event_timestamp         TIMESTAMP   NOT NULL,
    device_type             VARCHAR(20),
    session_start           TIMESTAMP,
    session_duration_seconds BIGINT,
    _ingested_at            TIMESTAMP   NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT pk_staging_clickstream PRIMARY KEY (session_id)
);
