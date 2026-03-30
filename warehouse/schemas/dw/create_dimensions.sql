-- create_dimensions.sql
-- Data Warehouse dimension and fact DDL for Contoso Fabric Platform
-- Uses surrogate keys (SK), business keys (BK), and SCD Type 2 patterns

CREATE SCHEMA IF NOT EXISTS dw;

-- ============================================================
-- dw.dim_date
-- ============================================================
CREATE TABLE IF NOT EXISTS dw.dim_date (
    date_sk         INTEGER         NOT NULL,
    full_date       TIMESTAMP       NOT NULL,
    year            INTEGER         NOT NULL,
    quarter         INTEGER         NOT NULL CHECK (quarter BETWEEN 1 AND 4),
    month           INTEGER         NOT NULL CHECK (month BETWEEN 1 AND 12),
    month_name      VARCHAR(10)     NOT NULL,
    day             INTEGER         NOT NULL CHECK (day BETWEEN 1 AND 31),
    day_of_week     INTEGER         NOT NULL CHECK (day_of_week BETWEEN 1 AND 7),
    day_name        VARCHAR(10)     NOT NULL,
    is_weekend      BOOLEAN         NOT NULL DEFAULT FALSE,
    CONSTRAINT pk_dim_date PRIMARY KEY (date_sk)
);

-- ============================================================
-- dw.dim_customer — SCD Type 2
-- ============================================================
CREATE TABLE IF NOT EXISTS dw.dim_customer (
    customer_sk     BIGINT          NOT NULL,
    customer_bk     VARCHAR         NOT NULL,
    name_hash       VARCHAR(64),
    email_hash      VARCHAR(64),
    city            VARCHAR,
    country         VARCHAR,
    signup_date     TIMESTAMP,
    _valid_from     TIMESTAMP       NOT NULL,
    _valid_to       TIMESTAMP,
    _is_current     BOOLEAN         NOT NULL DEFAULT TRUE,
    CONSTRAINT pk_dim_customer PRIMARY KEY (customer_sk),
    CONSTRAINT uq_dim_customer_bk_current UNIQUE (customer_bk, _is_current)
);

CREATE INDEX IF NOT EXISTS idx_dim_customer_bk ON dw.dim_customer (customer_bk);
CREATE INDEX IF NOT EXISTS idx_dim_customer_current ON dw.dim_customer (_is_current);

-- ============================================================
-- dw.dim_product — SCD Type 2
-- ============================================================
CREATE TABLE IF NOT EXISTS dw.dim_product (
    product_sk      BIGINT          NOT NULL,
    product_bk      VARCHAR         NOT NULL,
    product_name    VARCHAR,
    category        VARCHAR,
    sub_category    VARCHAR,
    unit_cost       DECIMAL(12, 2),
    _valid_from     TIMESTAMP       NOT NULL,
    _valid_to       TIMESTAMP,
    _is_current     BOOLEAN         NOT NULL DEFAULT TRUE,
    CONSTRAINT pk_dim_product PRIMARY KEY (product_sk)
);

CREATE INDEX IF NOT EXISTS idx_dim_product_bk ON dw.dim_product (product_bk);

-- ============================================================
-- dw.dim_store — SCD Type 2
-- ============================================================
CREATE TABLE IF NOT EXISTS dw.dim_store (
    store_sk        BIGINT          NOT NULL,
    store_bk        VARCHAR         NOT NULL,
    store_name      VARCHAR,
    city            VARCHAR,
    country         VARCHAR,
    region          VARCHAR,
    _valid_from     TIMESTAMP       NOT NULL,
    _valid_to       TIMESTAMP,
    _is_current     BOOLEAN         NOT NULL DEFAULT TRUE,
    CONSTRAINT pk_dim_store PRIMARY KEY (store_sk)
);

-- ============================================================
-- dw.fact_sales
-- ============================================================
CREATE TABLE IF NOT EXISTS dw.fact_sales (
    transaction_id  VARCHAR         NOT NULL,
    customer_sk     BIGINT,
    product_sk      BIGINT,
    store_sk        BIGINT,
    date_sk         INTEGER,
    quantity        INTEGER         NOT NULL,
    unit_price      DECIMAL(12, 2)  NOT NULL,
    total_amount    DECIMAL(14, 2)  NOT NULL,
    payment_method  VARCHAR(20),
    _processed_at   TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT pk_fact_sales PRIMARY KEY (transaction_id),
    CONSTRAINT fk_fact_sales_customer FOREIGN KEY (customer_sk) REFERENCES dw.dim_customer (customer_sk),
    CONSTRAINT fk_fact_sales_product FOREIGN KEY (product_sk) REFERENCES dw.dim_product (product_sk),
    CONSTRAINT fk_fact_sales_date FOREIGN KEY (date_sk) REFERENCES dw.dim_date (date_sk)
);

CREATE INDEX IF NOT EXISTS idx_fact_sales_date_sk ON dw.fact_sales (date_sk);
CREATE INDEX IF NOT EXISTS idx_fact_sales_customer_sk ON dw.fact_sales (customer_sk);

-- ============================================================
-- dw.fact_inventory_snapshot
-- ============================================================
CREATE TABLE IF NOT EXISTS dw.fact_inventory_snapshot (
    snapshot_id     VARCHAR         NOT NULL,
    warehouse_id    VARCHAR         NOT NULL,
    product_sk      BIGINT,
    date_sk         INTEGER,
    units_in        INTEGER         NOT NULL DEFAULT 0,
    units_out       INTEGER         NOT NULL DEFAULT 0,
    net_movement    INTEGER         NOT NULL DEFAULT 0,
    _processed_at   TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT pk_fact_inventory_snapshot PRIMARY KEY (snapshot_id)
);
