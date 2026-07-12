# NYC Taxi Lakehouse Demo

A minimal end-to-end data lakehouse built with **NYC Yellow Taxi** data using **Apache Iceberg**, **PyArrow**, **PyIceberg**, **DuckDB**, and **Streamlit**.

This is a personal learning project built to understand modern data lakehouse architecture from end to end. It demonstrates how raw data is ingested, validated, transformed into Bronze, Silver, and Gold layers, and exposed through SQL analytics and interactive dashboards using open-source technologies.

---

## Features

- End-to-end Bronze → Silver → Gold pipeline
- Apache Iceberg table format
- Local Iceberg catalog
- Data-quality validation and quarantine layer
- Business analytics Gold tables
- DuckDB SQL analytics
- Interactive Streamlit dashboard
- Engineering insight dashboard
- Idempotent pipelines
- Metadata logging

---

## Interesting Findings

During development, several interesting characteristics of the NYC Yellow Taxi dataset were discovered and incorporated into the pipeline.

- **27.92% of trips use `payment_type = 0 (Flex Fare trip)`**, and every one of these records has `passenger_count`, `RatecodeID`, `airport_fee`, `congestion_surcharge`, and `store_and_fwd_flag` set to `NULL`. Rather than treating these rows as corrupted, the pipeline preserves them as a distinct record format and profiles their behavior separately.

- **Monthly source files are not perfectly bounded by calendar month.** A total of **39 valid trips** have pickup timestamps outside the month indicated by their source file. These trips occur around month boundaries (late-night trips at the end of previous month or early trips at the beginning of the next month), so the pipeline preserves them and records a source-month offset for lineage and analysis.

- **1.17% of records are quarantined** because they violate data-quality rules, including negative monetary values, invalid passenger counts, timestamp inconsistencies, or pickups outside the project's accepted date range.

- **The dataset contains thousands of anomalous but potentially valid trips**, including zero-distance trips, zero-duration trips, zero fares, zero total amounts, and trips where `total_amount < fare_amount`. These records suggest that individual fields may contain recording errors while the remaining attributes are still usable, making it difficult to determine whether the entire record should be discarded. Because the TLC documentation does not explicitly classify these cases as invalid, the pipeline keeps them in Silver with warning flags and exposes four Gold data variants so their impact on downstream analytics can be evaluated.

---

# Architecture

```text
NYC Taxi Parquet Files
          │
          ▼
┌──────────────────────────┐
│ Bronze                   │
│ Raw ingestion            │
└──────────────────────────┘
          │
          ▼
┌──────────────────────────┐
│ Silver                   │
│ Cleaning                 │
│ Validation               │
│ Standardization          │
│ Quarantine               │
└──────────────────────────┘
          │
          ▼
┌──────────────────────────┐
│ Gold                     │
│ Business Metrics         │
│ KPI Tables               │
│ Data Quality Tables      │
└──────────────────────────┘
          │
          ├─────────────► DuckDB SQL
          │
          └─────────────► Streamlit Dashboard
```

---

# Project Structure

```text
configs/

data/
    raw/
    warehouse/

pipelines/
    ingest_bronze.py
    transform_silver_with_quarantine.py
    build_gold.py

dashboard/

sql/

notebooks/

scripts/
```

---

# Technologies

| Category | Technology |
|----------|------------|
| Table Format | Apache Iceberg |
| Processing | PyArrow |
| Catalog | PyIceberg + SQLite |
| SQL | DuckDB |
| Dashboard | Streamlit |
| Storage | Local Parquet |
| Language | Python |

---

# Data Pipeline

## Bronze

### Features

- Reads raw NYC Taxi Parquet files
- Preserves original schema
- Stores ingestion metadata
- Fully idempotent

### Output

```
bronze.yellow_trips
```

---

## Silver

Cleans and validates Bronze data.

### Features

- Schema normalization
- Timestamp validation
- Monetary amount validation
- Data quality flags
- Quarantine invalid records
- Source month-offset lineage tracking
- Partitioned by pickup month for efficient analytical queries

### Outputs

```text
silver.yellow_trips
quarantine.yellow_trips_rejected
```

---

## Gold

Produces analytical tables for BI and dashboards.

### Feature
Current Gold layer includes

- Daily KPI
- Monthly KPI
- Pickup zones
- Dropoff zones
- Zone flows
- Payment analysis
- Vendor analysis
- Payment null profile
- Data quality summary

### Outputs
All Gold tables are rebuilt from Silver on every execution.

---

# Gold Dimensions

Most monthly Gold tables support two independent analytical dimensions.

## Data Variant

```
all
exclude_zero_duration
exclude_zero_distance
exclude_both
```

This allows business metrics to be compared before and after removing zero-value trips without rebuilding Silver.

---

## Month Basis

```
pickup_month
source_month
```

pickup_month

- groups by pickup timestamp

source_month

- groups by original raw file month

This makes month-offset analysis possible while preserving business reporting.

---

# Streamlit Dashboard

The dashboard contains four pages.

## Overview

- Daily trips
- Revenue
- Monthly KPI
- Trend analysis

## Zone Flows

- Pickup statistics
- Dropoff statistics
- Origin → Destination flow matrix

## Payment & Vendor

- Payment mix
- Vendor comparison
- Payment type 0 analysis
- Zone flow by payment/vendor

## Data Quality

- Pipeline funnel
- Rejected rows
- Warning metrics
- Zero-value analysis
- Month-offset analysis
- Payment type 0 evidence

---

# SQL Analytics

Reusable DuckDB SQL queries are included for every dashboard view.

```
sql/

daily_metrics.sql
monthly_kpi.sql
zone_analysis.sql
payment_analysis.sql
vendor_analysis.sql
data_quality.sql
payment_null_profile.sql
```

---

# Running

## 1. Install dependencies

```bash
pip install -r requirements.txt
```

---

## 2.1 Run everything

Downloads the configured NYC Taxi datasets, builds the Bronze, Silver, and Gold layers, then launches the Streamlit dashboard.

```bash
python scripts/run_all.py
```

To skip launching the dashboard:

```bash
python scripts/run_all.py --no-dashboard
```

---

## 2.2 Run each step manually

### Download datasets

```bash
python scripts/download_dataset.py
```

### Build Bronze

```bash
python pipelines/ingest_bronze.py
```

### Build Silver

```bash
python pipelines/transform_silver_with_quarantine.py
```

### Build Gold

```bash
python pipelines/build_gold.py
```

### Launch dashboard

```bash
streamlit run dashboard/app.py
```

---

# Current Dataset

- NYC Yellow Taxi
- January–March 2026
- ~11 million trips

---

# Roadmap

## ~~Phase 1 — Minimal Lakehouse~~

- [x] Bronze
- [x] Silver
- [x] Gold
- [x] Iceberg catalog
- [x] SQL analytics
- [x] Streamlit dashboard
- [x] Data-quality dashboard

---

## Phase 2 — Reliability & Infrastructure

- [ ] Recovery and checkpoints
- [ ] Improved idempotency
- [ ] Enhanced monitoring
- [ ] Query benchmarking
- [ ] MinIO
- [ ] Docker Compose
- [ ] CI/CD

---

## Phase 3 — Lakehouse Expansion

- [ ] Green Taxi
- [ ] FHV
- [ ] HVFHV
- [ ] Cross-dataset Gold
- [ ] Borough enrichment
- [ ] Historical trends

---

## Future Ideas

- Documentation
- PySpark pipelines
- Apache Spark SQL
- Apache Nessie
- Airflow
- Kubernetes
- AWS S3
- Streaming ingestion
- Great Expectations
- dbt
- Real-time dashboard