# Technical Decisions & Architecture Solutions - DataMart ETL Pipeline

This document outlines the core architectural decisions, data engineering patterns, and problem-solving strategies implemented to build a robust, scalable, and idempotent ETL pipeline.

---

## 1. Multi-Source Ingestion & Unification
* **Challenge**: The project required handling two distinct transactional datasets: a daily operational stream (`data.csv`) and a large-scale historical dump (`online_retail.csv`). 
* **Solution**: Designed a dynamic extraction and transformation pipeline in Apache Airflow using Pandas. Both files are ingested into unified memory dataframes, standardizing column names, data types, and timestamps into UTC before merging. Overlapping date ranges are cleaned using composite subset deduplication (`InvoiceNo`, `StockCode`, `InvoiceDate`, `Quantity`) to prevent record inflation and financial metric distortion.

## 2. Dimensional Modeling (Star Schema)
* **Challenge**: Raw transactional data contains denormalized redundancy, mixing customer details, product attributes, and transaction lines.
* **Solution**: Implemented a dimensional **Star Schema** inside PostgreSQL (`postgres-dw`) consisting of:
  * **Dimensions (`dim_customers`, `dim_products`)**: Decoupled entity attributes to reduce storage footprint and optimize analytical joins.
  * **Fact Tables (`fact_sales`, `fact_returns`)**: Split financial events based on quantity rules (Sales where `Quantity > 0`, Returns where `Quantity <= 0`), ensuring clean aggregation metrics and accurate revenue calculations.
  * **Audit Log (`log_rejections`)**: Isolated corrupted or anomalous records (e.g., negative unit prices) into a separate rejection buffer, maintaining data quality without crashing pipeline execution.

## 3. Strict Idempotency & Fault Tolerance
* **Challenge**: In distributed data pipelines, network timeouts or manual retries can cause duplicate inserts, corrupting data warehouses.
* **Solution**: Built strict idempotency at the database and code level:
  * Dimension tables enforce `ON CONFLICT DO NOTHING` on primary keys.
  * Fact tables leverage composite unique constraints (`invoice_no`, `product_code`, `invoice_date`/`return_date`). Re-running the DAG multiple times results in an identical, non-duplicated state.

## 4. High-Performance Bulk Loading
* **Challenge**: Inserting hundreds of thousands of rows row-by-row using standard SQL cursors creates severe performance bottlenecks.
* **Solution**: Integrated `psycopg2.extras.execute_values` for bulk multi-row insertions in PostgreSQL, accelerating load times for over 500,000 net records to under 60 seconds.

## 5. Ambiguity Resolution & Data Hygiene
* **Challenge**: Missing customer IDs (`CustomerID` as NaN/null) and inconsistent product descriptions (mixed casing, trailing spaces).
* **Solution**: 
  * Missing customers are systematically mapped to an `UNKNOWN` surrogate record in `dim_customers` rather than dropped, preserving true financial totals.
  * Product descriptions are cleaned, standardized to uppercase, and stripped of whitespaces, keeping the first historical record as the canonical description.