# Contoso Fabric Platform — Data Dictionary

## Bronze Layer Tables

### `bronze.pos_transactions`
**Layer:** Bronze
**Source:** CSV files in `lakehouse/bronze/raw/pos_transactions/`
**Owner:** data-engineering-team
**Notebook:** `notebooks/bronze/ingest_pos_transactions.py`
**Description:** Raw POS transaction records as ingested from source CSV files.

| Column | Type | Nullable | Description | Source |
|--------|------|----------|-------------|--------|
| transaction_id | STRING | Yes | Unique transaction identifier | Source CSV |
| store_id | STRING | Yes | Store where the transaction occurred | Source CSV |
| product_id | STRING | Yes | Product purchased | Source CSV |
| customer_id | STRING | Yes | Customer who made the purchase (may be null for cash) | Source CSV |
| quantity | STRING | Yes | Number of units purchased (raw string from CSV) | Source CSV |
| unit_price | STRING | Yes | Price per unit (raw string from CSV) | Source CSV |
| sale_date | STRING | Yes | Transaction date/time (raw string) | Source CSV |
| payment_method | STRING | Yes | Method of payment | Source CSV |
| _ingested_at | TIMESTAMP | Yes | When the record was ingested by the pipeline | Platform |
| _source_file | STRING | Yes | Source file path that produced this record | Platform |

### `bronze.customers`
**Layer:** Bronze
**Source:** CSV files in `lakehouse/bronze/raw/customers/`
**Owner:** data-engineering-team
**Notebook:** `notebooks/bronze/ingest_customers.py`
**Description:** Raw customer records including PII. PII is hashed in Silver.

| Column | Type | Nullable | Description | Source |
|--------|------|----------|-------------|--------|
| customer_id | STRING | Yes | Unique customer identifier | Source CSV |
| name | STRING | Yes | Customer full name (PII — hashed in Silver) | Source CSV |
| email | STRING | Yes | Customer email address (PII — hashed in Silver) | Source CSV |
| phone | STRING | Yes | Customer phone number (PII — hashed in Silver) | Source CSV |
| address | STRING | Yes | Customer street address (PII — hashed in Silver) | Source CSV |
| city | STRING | Yes | Customer city | Source CSV |
| country | STRING | Yes | Customer country | Source CSV |
| signup_date | STRING | Yes | Date customer joined (raw string) | Source CSV |
| _ingested_at | TIMESTAMP | Yes | Ingestion timestamp | Platform |
| _source_file | STRING | Yes | Source file path | Platform |

---

## Silver Layer Tables

### `silver.pos_transactions`
**Layer:** Silver
**Source:** `lakehouse/bronze/pos_transactions/`
**Owner:** data-engineering-team
**Notebook:** `notebooks/silver/clean_transactions.py`
**Description:** Cleaned and typed POS transactions. Duplicates removed, types enforced.

| Column | Type | Nullable | Description | Source |
|--------|------|----------|-------------|--------|
| transaction_id | STRING | No | Unique transaction identifier | bronze.transaction_id |
| store_id | STRING | No | Store identifier | bronze.store_id |
| product_id | STRING | No | Product identifier | bronze.product_id |
| customer_id | STRING | Yes | Customer identifier (null for anonymous) | bronze.customer_id |
| quantity | INT | No | Number of units purchased | bronze.quantity (cast) |
| unit_price | DECIMAL(12,2) | No | Price per unit | bronze.unit_price (cast) |
| sale_date | TIMESTAMP | No | Transaction timestamp | bronze.sale_date (cast) |
| payment_method | STRING | No | Payment method (normalised to UPPER) | bronze.payment_method |
| total_amount | DECIMAL(14,2) | Yes | Computed: quantity × unit_price | Computed |
| _processed_at | TIMESTAMP | No | When this record was processed | Platform |

**Business Rules:**
- Rows with null transaction_id, store_id, or product_id are filtered out
- quantity must be > 0
- unit_price must be > 0.01
- payment_method normalised to uppercase

### `silver.customers`
**Layer:** Silver
**Source:** `lakehouse/bronze/customers/`
**Owner:** data-engineering-team
**Notebook:** `notebooks/silver/clean_customers.py`
**Description:** Cleaned customer data with PII columns replaced by SHA-256 hashes.

| Column | Type | Nullable | Description | Source |
|--------|------|----------|-------------|--------|
| customer_id | STRING | No | Unique customer identifier | bronze.customer_id |
| name_hash | STRING | Yes | SHA-256 hash of customer name | hash(bronze.name) |
| email_hash | STRING | Yes | SHA-256 hash of email address | hash(bronze.email) |
| phone_hash | STRING | Yes | SHA-256 hash of phone number | hash(bronze.phone) |
| address_hash | STRING | Yes | SHA-256 hash of street address | hash(bronze.address) |
| city | STRING | Yes | Customer city (not PII) | bronze.city |
| country | STRING | Yes | Customer country (not PII) | bronze.country |
| signup_date | TIMESTAMP | Yes | Customer registration timestamp | bronze.signup_date (cast) |
| _processed_at | TIMESTAMP | No | Processing timestamp | Platform |

---

## Gold Layer Tables

### `gold.dim_customer`
**Layer:** Gold
**Source:** `lakehouse/silver/customers/`
**Owner:** data-engineering-team
**Notebook:** `notebooks/gold/build_dim_customer.py`
**Description:** Customer dimension with SCD Type 2 history tracking.

| Column | Type | Nullable | Description | Source |
|--------|------|----------|-------------|--------|
| customer_sk | BIGINT | No | Surrogate key (auto-generated) | Platform |
| customer_bk | STRING | No | Business key (= customer_id) | silver.customer_id |
| name_hash | STRING | Yes | SHA-256 hash of name | silver.name_hash |
| email_hash | STRING | Yes | SHA-256 hash of email | silver.email_hash |
| city | STRING | Yes | Customer city | silver.city |
| country | STRING | Yes | Customer country | silver.country |
| signup_date | TIMESTAMP | Yes | Registration date | silver.signup_date |
| _valid_from | TIMESTAMP | No | When this version became active | Platform |
| _valid_to | TIMESTAMP | Yes | When this version was superseded (null = current) | Platform |
| _is_current | BOOLEAN | No | True if this is the current version | Platform |

### `gold.fact_sales`
**Layer:** Gold
**Source:** `lakehouse/silver/pos_transactions/`, `gold.dim_customer`, `gold.dim_product`
**Owner:** data-engineering-team
**Notebook:** `notebooks/gold/build_fact_sales.py`
**Description:** Sales fact table in star schema format.

| Column | Type | Nullable | Description | Source |
|--------|------|----------|-------------|--------|
| transaction_id | STRING | No | Grain: one row per transaction | silver.transaction_id |
| customer_sk | BIGINT | Yes | Customer surrogate key | dim_customer |
| product_sk | BIGINT | Yes | Product surrogate key | dim_product |
| store_sk | BIGINT | Yes | Store surrogate key | dim_store |
| date_sk | INT | Yes | Date surrogate key (yyyyMMdd) | Derived from sale_date |
| quantity | INT | No | Units sold | silver.quantity |
| unit_price | DECIMAL(12,2) | No | Price per unit | silver.unit_price |
| total_amount | DECIMAL(14,2) | No | Total transaction amount | silver.total_amount |
| payment_method | STRING | Yes | Payment method | silver.payment_method |
| _processed_at | TIMESTAMP | No | Processing timestamp | Platform |

### `gold.agg_customer_360`
**Layer:** Gold
**Source:** `lakehouse/silver/pos_transactions/`
**Owner:** data-engineering-team
**Notebook:** `notebooks/gold/build_customer_360.py`
**Description:** Customer 360 aggregate — lifetime value, purchase behaviour.

| Column | Type | Nullable | Description | Source |
|--------|------|----------|-------------|--------|
| customer_bk | STRING | No | Business key (= customer_id) | silver.customer_id |
| total_orders | BIGINT | No | Total number of transactions | COUNT(transaction_id) |
| total_spend | DECIMAL(16,2) | No | Cumulative spend | SUM(total_amount) |
| avg_order_value | DECIMAL(12,2) | Yes | Average transaction value | AVG(total_amount) |
| first_purchase_date | TIMESTAMP | Yes | Date of first purchase | MIN(sale_date) |
| last_purchase_date | TIMESTAMP | Yes | Date of most recent purchase | MAX(sale_date) |
| preferred_payment_method | STRING | Yes | Most frequently used payment method | MODE(payment_method) |
| lifetime_value_tier | STRING | Yes | BRONZE/SILVER/GOLD/PLATINUM based on total_spend | Business rule |
| _processed_at | TIMESTAMP | No | Processing timestamp | Platform |

**Business Rules:**
- BRONZE: total_spend < 1,000
- SILVER: total_spend 1,000–4,999
- GOLD: total_spend 5,000–9,999
- PLATINUM: total_spend ≥ 10,000
