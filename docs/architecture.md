# Contoso Fabric Platform — Architecture

## Overview
The Contoso Fabric Platform implements the **medallion architecture** (Bronze → Silver → Gold)
using PySpark and Delta Lake, simulating a Microsoft Fabric environment locally.

## Data Flow

```mermaid
flowchart LR
    subgraph Sources["📥 Data Sources"]
        POS["POS Transactions\nCSV"]
        INV["Inventory Movements\nCSV"]
        CUST["Customers\nCSV"]
        CLICK["Web Clickstream\nJSON"]
    end

    subgraph Bronze["🟫 Bronze Layer\nlakehouse/bronze/"]
        B1["pos_transactions\n(Delta)"]
        B2["inventory_movements\n(Delta)"]
        B3["customers\n(Delta)"]
        B4["clickstream\n(Delta)"]
    end

    subgraph Silver["⬜ Silver Layer\nlakehouse/silver/"]
        S1["pos_transactions\n(cleaned, typed)"]
        S2["inventory_movements\n(validated)"]
        S3["customers\n(PII hashed)"]
        S4["clickstream\n(sessionised)"]
    end

    subgraph Gold["🟡 Gold Layer\nlakehouse/gold/"]
        direction TB
        D1["dim_customer"]
        D2["dim_product"]
        D3["dim_store"]
        D4["dim_date"]
        F1["fact_sales"]
        F2["fact_inventory_snapshot"]
        A1["agg_customer_360"]
    end

    subgraph Warehouse["🏭 DuckDB Warehouse"]
        W1["dw schema\n(star schema)"]
        W2["reporting views"]
    end

    subgraph BI["📊 Analytics"]
        BI1["Power BI / Reports"]
    end

    POS --> B1
    INV --> B2
    CUST --> B3
    CLICK --> B4

    B1 --> S1
    B2 --> S2
    B3 --> S3
    B4 --> S4

    S1 --> F1
    S1 --> D2
    S1 --> A1
    S3 --> D1
    S2 --> F2

    D1 --> F1
    D2 --> F1
    D4 --> F1
    F1 --> W1
    D1 --> W1
    D2 --> W1

    W1 --> W2
    W2 --> BI1
    A1 --> BI1
```

## Layer Descriptions

### Bronze Layer
**Purpose:** Raw data ingestion — land data exactly as received from source systems.

**Rules:**
- No business transformations
- Add `_ingested_at` (current timestamp) and `_source_file` (source path) columns
- Schema permissive: accept whatever comes from source
- All columns stored as strings (type enforcement happens in Silver)
- Write using Delta upsert on natural business key

**Location:** `lakehouse/bronze/{entity}/`

### Silver Layer
**Purpose:** Data cleaning, validation, and standardisation.

**Rules:**
- Deduplicate on business key
- Cast all columns to correct types per schema registry
- Handle nulls: filter rows with null business keys, fill defaults elsewhere
- Hash PII columns (email, phone, address, name) using SHA-256
- Drop original PII columns
- Write using Delta merge on business key

**Location:** `lakehouse/silver/{entity}/`

### Gold Layer
**Purpose:** Business-ready data products for analytics and reporting.

**Rules:**
- Build star schema: dimensions + facts
- Add surrogate keys (SK), SCD Type 2 columns on dimensions
- Join Silver tables to create enriched facts
- Build aggregates (customer 360, etc.)
- No raw PII data

**Location:** `lakehouse/gold/{entity}/`

## Pipeline DAG

```mermaid
graph TD
    B1[Bronze: POS Transactions] --> DQ_B[Bronze DQ Checkpoint]
    B2[Bronze: Inventory] --> DQ_B
    B3[Bronze: Customers] --> DQ_B
    B4[Bronze: Clickstream] --> DQ_B

    DQ_B --> S1[Silver: Clean Transactions]
    DQ_B --> S2[Silver: Clean Inventory]
    DQ_B --> S3[Silver: Clean Customers]
    DQ_B --> S4[Silver: Sessionise Clicks]

    S1 --> DQ_S[Silver DQ Checkpoint]
    S2 --> DQ_S
    S3 --> DQ_S
    S4 --> DQ_S

    DQ_S --> G_DC[Gold: dim_customer]
    DQ_S --> G_DP[Gold: dim_product]
    DQ_S --> G_C360[Gold: agg_customer_360]

    G_DC --> G_FS[Gold: fact_sales]
    G_DP --> G_FS

    G_FS --> DQ_G[Gold DQ Checkpoint]
    G_C360 --> DQ_G
```

## Technology Stack

| Component | Technology |
|-----------|-----------|
| Processing | PySpark 3.5 |
| Storage format | Delta Lake 3.x |
| Orchestration | Pipeline YAML (custom) |
| Data Quality | Great Expectations |
| Warehouse | DuckDB |
| Testing | pytest + chispa |
| Linting | ruff + mypy |
| CI/CD | GitHub Actions |
| Local dev | Docker Compose |
