-- create_views.sql
-- Reporting views for Contoso Fabric Platform DuckDB warehouse

CREATE SCHEMA IF NOT EXISTS reporting;

-- ============================================================
-- reporting.v_sales_summary
-- Daily sales summary by store and product category
-- ============================================================
CREATE OR REPLACE VIEW reporting.v_sales_summary AS
WITH
    daily_sales AS (
        SELECT
            fs.date_sk,
            fs.store_sk,
            fs.product_sk,
            SUM(fs.total_amount) AS daily_revenue,
            SUM(fs.quantity) AS daily_units_sold,
            COUNT(fs.transaction_id) AS transaction_count
        FROM
            dw.fact_sales AS fs
        GROUP BY
            fs.date_sk,
            fs.store_sk,
            fs.product_sk
    ),
    enriched AS (
        SELECT
            dd.full_date,
            dd.year,
            dd.month,
            dd.month_name,
            ds.store_bk AS store_id,
            ds.store_name,
            ds.region,
            dp.category,
            dp.sub_category,
            dls.daily_revenue,
            dls.daily_units_sold,
            dls.transaction_count
        FROM
            daily_sales AS dls
            LEFT JOIN dw.dim_date AS dd ON dls.date_sk = dd.date_sk
            LEFT JOIN dw.dim_store AS ds ON dls.store_sk = ds.store_sk AND ds._is_current
            LEFT JOIN dw.dim_product AS dp ON dls.product_sk = dp.product_sk AND dp._is_current
    )
SELECT
    *
FROM enriched;

-- ============================================================
-- reporting.v_customer_360
-- Customer 360 view joining aggregates with dimension
-- ============================================================
CREATE OR REPLACE VIEW reporting.v_customer_360 AS
WITH
    customer_metrics AS (
        SELECT
            c360.customer_bk,
            c360.total_orders,
            c360.total_spend,
            c360.avg_order_value,
            c360.first_purchase_date,
            c360.last_purchase_date,
            c360.preferred_payment_method,
            c360.lifetime_value_tier,
            dc.city,
            dc.country,
            dc.signup_date
        FROM
            dw.dim_customer AS dc
            INNER JOIN staging.customers AS c360
                ON dc.customer_bk = c360.customer_id
        WHERE
            dc._is_current
    )
SELECT * FROM customer_metrics;
