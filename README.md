# Urban Mobility Analytics

Data Engineering portfolio project built on **Databricks (Azure)** that simulates the data platform of an e-scooter and bike sharing company operating across three major cities in Mexico.

The goal is to demonstrate a production-grade, end-to-end data architecture combining a fully dynamic Medallion pipeline, object-oriented design, metadata-driven configuration, and automated data quality enforcement.

---

## Architecture Overview

```
ADLS Gen2 (Landing)
        │
        ▼
┌──────────────────────────────────────────────────────────────────────┐
│                    Databricks + Unity Catalog                         │
│                                                                      │
│  [00] Metadata Generator                                             │
│       Schema inference + YAML config generation                      │
│                │                                                     │
│                ▼                                                     │
│  [01] Bronze Loader     ──► bronze.{entity}      ◄── [04] Streaming │
│       Raw ingestion          + dq_quarantine           Auto Loader   │
│       + audit columns        + dq_run_results          (ride_events) │
│                │             + dq_referential_integrity              │
│                ▼                      │                              │
│  [02] Silver Cleaner    ──► silver.{entity}                          │
│       YAML-driven transforms                                         │
│       + quarantine + DQ metrics                                      │
│                │                                                     │
│                ▼                                                     │
│  [03] Gold Aggregator   ──► gold.{agg_name} (11 tables)             │
│       Business aggregations (trips · users · vehicles                │
│       maintenance_logs · ride_events)                                │
└──────────────────────────────────────────────────────────────────────┘
        │                         │                        │
        ▼                         ▼                        ▼
 AI/BI Dashboard            AI/BI Dashboard           Genie Space
 Data Quality Health        Business Intelligence      Natural Language
 (Silver + DQ tables)       (Gold layer — 11 tables)  (Gold layer — 11 tables)
```

---

## Tech Stack

| Layer | Technology |
|---|---|
| Cloud Platform | Microsoft Azure |
| Data Platform | Databricks |
| Storage | ADLS Gen2 |
| Table Format | Delta Lake |
| Catalog | Unity Catalog |
| Language | Python · PySpark |
| Orchestration | Databricks Jobs |
| Configuration | YAML (per-entity) |

---

## Entities

| Entity | Format | Source System | Bronze | Silver | Pass Rate |
|---|---|---|---|---|---|
| trips | CSV | Billing legacy | 5,050 | 4,770 | 94.5% |
| users | JSON | User registration API | 500 | 500 | 100% |
| vehicles | Parquet | Fleet management system | 200 | 200 | 100% |
| maintenance_logs | CSV | External workshop reports | 1,500 | 1,431 | 95.4% |
| ride_events | JSON Lines | Real-time telemetry | 51,469 | 47,294 | 91.9% |

---

## Project Structure

```
urban_mobility_project/
├── helpers/
│   ├── metadata_framework     # MetaSchema · EntityConfig · SilverRulesConfig · DataProfiler
│   ├── transformations        # 44 OOP transformations + TransformationEngine (Registry Pattern)
│   ├── validations            # DQEngine → writes metrics to bronze.dq_run_results
│   └── pipeline_base          # LayerProcessor (ABC) · BronzeLoader · SilverCleaner · GoldAggregator
│
├── notebooks/
│   ├── 00_metadata_generator  # Infers schema, profiles data, generates YAML configs
│   ├── 01_bronze_loader       # Ingests Landing → Bronze with audit columns
│   ├── 02_silver_cleaner      # Applies YAML-driven transformations + DQ
│   └── 03_gold_aggregator     # Generates Gold aggregation tables
│
└── config/
    ├── trips/
    │   ├── entity_config.yml  # Structural config + gold aggregations
    │   └── silver_rules.yml   # Transformation rules for Silver
    ├── users/
    ├── vehicles/
    ├── maintenance_logs/
    └── ride_events/
```

---

## Key Design Decisions

### 1 — Metadata-Driven Pipeline

The entire pipeline is driven by two YAML files per entity. No hardcoded logic per entity — adding a new data source only requires running the metadata generator.

```yaml
# silver_rules.yml (auto-generated + manually extensible)
transformations:
  - type: clean_currency
    column: fare_mxn
  - type: normalize_values
    column: city
    mapping: {cdmx: CDMX, MTY: Monterrey, GDL: Guadalajara}
  - type: quarantine_nulls
    columns: [trip_id, user_id]
    on_failure: quarantine
  - type: deduplicate
    keys: [trip_id]
  - type: add_derived_column
    output_col: duration_minutes
    expression: "(unix_timestamp(end_time) - unix_timestamp(start_time)) / 60"
```

### 2 — Object-Oriented Design

The pipeline uses four OOP patterns to maximize reusability and extensibility:

| Pattern | Implementation |
|---|---|
| **Template Method** | `LayerProcessor.run()` defines the fixed flow: `read → process → write` |
| **Registry Pattern** | `TransformationEngine.REGISTRY` maps 44 YAML type strings to Python classes |
| **Composition** | `SilverCleaner` HAS-A `TransformationEngine` and HAS-A `DQEngine` |
| **Abstract Classes** | `LayerProcessor` and `BaseTransformation` enforce contracts via ABCs |

```
LayerProcessor (ABC)
├── BronzeLoader         → reads Landing, adds audit columns
├── SilverCleaner        → applies transformations + DQ
│   ├── HAS-A: TransformationEngine
│   └── HAS-A: DQEngine
└── GoldAggregator       → generates business aggregation tables

BaseTransformation (ABC)
├── FilterTransformation (ABC)   → on_failure: drop | quarantine
│   ├── QuarantineNulls
│   ├── ClampNumeric
│   ├── ValidateEnum
│   └── FixTimestampOrder
├── CleanCurrency
├── NormalizeValues
├── NormalizeDateFormat
├── DeduplicateTransformation
├── AddDerivedColumn
└── ... (44 total)
```

### 3 — Automated Data Profiling

The `DataProfiler` scans each entity's data at landing and automatically generates the silver transformation rules:

| Data signal detected | Rule generated |
|---|---|
| Numeric stored as string with `$` | `clean_currency` |
| Duplicate primary keys (~1%) | `deduplicate` |
| Categorical column with case/spelling variants | `normalize_values` |
| Mixed date formats (ISO + DD/MM/YYYY) | `normalize_date_format` |
| `start_time > end_time` violations | `fix_timestamp_order` |
| Percentage column with values > 100 | `clamp_numeric max_val=100` |
| Speed column with values > 120 km/h | `clamp_numeric max_val=120` |
| Null values in primary key columns | `quarantine_nulls` |

### 4 — Data Quality with Quarantine Pattern

Records that fail quality rules are not silently dropped. They are written to a dedicated quarantine table with their rejection reason, making data issues fully auditable.

```
bronze.trips_quarantine    ← records rejected during Silver processing
bronze.dq_run_results      ← per-rule metrics for every pipeline run
```

Each pipeline run generates a unique `pipeline_run_id = {entity}_{YYYYMMDD_HHMMSS}` that links Bronze audit columns, Silver DQ columns, and DQ metrics together.

### 5 — Audit Columns

**Bronze layer** adds:
```
_ingested_at       TIMESTAMP   — when the record was loaded
_source_file       STRING      — source file path in landing
_pipeline_run_id   STRING      — traceability ID
_layer             STRING      — always "bronze"
_record_hash       STRING      — MD5 hash of record content
```

**Silver layer** adds:
```
_processing_timestamp   TIMESTAMP   — when Silver processing ran
_dq_status              STRING      — passed | failed
_dq_notes               STRING      — description of any quality issue
```

---

## Intentional Data Inconsistencies

The dataset includes realistic data quality problems designed to test the pipeline:

| Entity | Issue | How it's handled |
|---|---|---|
| trips | `fare_mxn` stored as `"$45.00"` string (~10% of rows) | `clean_currency` → parsed to Double |
| trips | `city` with variants: "CDMX", "cdmx", "Ciudad de Mexico", "MTY", "GDL" | `normalize_values` → standardized |
| trips | `end_time < start_time` (inverted timestamps) | `fix_timestamp_order` → quarantine |
| trips | Null `user_id` | `quarantine_nulls` → quarantine |
| trips | Duplicate `trip_id` (~1%) | `deduplicate` |
| trips | `distance_km = 0` with `fare_mxn > 0` | `detect_impossible_combination` → flag column |
| users | `plan_type` variants: "premium", "Premium", "PREMIUM" | `normalize_values` → "Premium" |
| users | `signup_date` in ISO 8601 and DD/MM/YYYY mixed formats | `normalize_date_format` → `try_to_timestamp` |
| vehicles | Orphan `scooter_id` not present in trips | tracked via referential integrity checks |
| maintenance_logs | `cost_usd` with negative values | `clamp_numeric min_val=0` |
| maintenance_logs | Future `maintenance_date` (data entry errors) | `validate_date_range` |
| ride_events | `battery_pct` outside 0–100 range | `clamp_numeric min=0, max=100` |
| ride_events | `speed_kmh > 120` (impossible for a scooter) | `clamp_numeric max=120` |
| ride_events | Duplicate `event_id` (~1%) | `deduplicate` |

---

## Dashboards

### Data Quality Dashboard

An AI/BI dashboard provides real-time visibility into pipeline health across all 5 entities:

- **KPIs:** Total records per layer, Pass Rate %, Quarantine count, Flagged records, Average fare
- **Charts:** Layer flow (Bronze → Silver → Quarantine), City distribution, Trip categories, Payment methods
- **Trend:** Monthly trip volume over 18 months
- **Table:** Quarantine record detail with rejection context (consolidated `bronze.dq_quarantine` table)

### Business Intelligence Dashboard

A second AI/BI dashboard built on top of the Gold layer exposes the key business KPIs of the platform:

| Section | Content |
|---|---|
| **KPIs** | Total Revenue MXN · Total Trips · Avg Fare · Active Fleet · Total Users · Maintenance Cost USD |
| **Operations** | Revenue and trip count by city (CDMX / Monterrey / Guadalajara) |
| **Trends** | Monthly trip volume and revenue over 18 months |
| **Fleet** | Vehicle status breakdown · Fleet distribution by city |
| **Users** | User count by plan type (Basic / Premium) |
| **Maintenance** | Cost by issue type · Monthly maintenance spend trend |
| **Telemetry** | Event count by type · Hourly activity heatmap (peak hours: 7–9am, 6–8pm) |

Built on 11 Gold aggregation tables covering all 5 entities.

---

## Genie Space — Natural Language Analytics

A **Databricks Genie Space** is available on top of the Gold layer, enabling any user to explore the data through natural language without writing SQL.

**Connected tables:** all 11 Gold tables (trips, users, vehicles, maintenance_logs, ride_events)

Sample questions the Genie Space can answer:

- *¿Cuál ciudad genera más ingresos totales?*
- *¿A qué hora del día hay más actividad en la flota?*
- *¿Qué tipo de mantenimiento tiene el mayor costo promedio?*
- *¿Cómo han evolucionado los viajes mes a mes?*
- *¿Cuánto revenue total se generó en los últimos 6 meses?*

---

## How to Run

Each entity has its own Databricks Job with four sequential tasks. To process a new entity or re-process an existing one:

```bash
# Trigger via Databricks CLI
databricks jobs run-now <JOB_ID> --profile personal
```

Or use the Databricks UI — each job accepts `entity_name` and all source parameters as widget inputs.

**Important:** Always use a classic cluster (`existing_cluster_id`). Serverless compute accumulates significant cost even when idle.

---

## Operational Cities

| City | Share | Approximate Coordinates |
|---|---|---|
| CDMX | 50% | 19.4326° N, 99.1332° W |
| Monterrey | 30% | 25.6866° N, 100.3161° W |
| Guadalajara | 20% | 20.6597° N, 103.3496° W |

---

## Advanced Architecture Features

### 6 — MERGE/UPSERT for Incremental Loads

`LayerProcessor.write()` routes automatically based on `system.load_type` in entity_config.yml:

| load_type | Behavior |
|---|---|
| `INCREMENTAL LOAD` | Delta MERGE by PK — INSERT new, UPDATE existing |
| `FULL LOAD` | overwrite |
| `APPEND` | append-only |

Bronze always overwrites (raw layer preserves duplicates by design). The `_merge()` method uses `col_map = {c: F.col(f"source.{c}") for c in df.columns}` on both UPDATE and INSERT clauses — gracefully handles schema evolution between runs.

### 7 — Streaming Pipeline (ride_events)

Dedicated notebook `04_streaming_ride_events` using **Auto Loader** + **Structured Streaming**:

- `cloudFiles.format: json` auto-detects new files in ADLS landing folder
- Schema inference persisted to ADLS checkpoint for stability
- `trigger(availableNow=True)` — processes all pending files and terminates (job-compatible)
- Silver uses `foreachBatch` + Delta MERGE by `event_id` for idempotent processing
- Reuses `TransformationEngine` for consistency with the batch pipeline
- Checkpoints stored in ADLS for stateful incremental processing

### 8 — Cross-Entity Referential Integrity

`05_referential_integrity` notebook validates FK relationships across Silver tables:

| Relationship | Orphans | Pass Rate |
|---|---|---|
| `ride_events.trip_id → trips.trip_id` | 1,932 | 95.9% |
| `ride_events.scooter_id → vehicles.scooter_id` | 796 | 98.3% |
| `maintenance_logs.scooter_id → vehicles.scooter_id` | 77 | 94.6% |

Results persisted to `bronze.dq_referential_integrity` with traceable `run_id`.

### 9 — Liquid Clustering

`CLUSTER BY` applied to all large tables (replacing PARTITION BY + ZORDER):

```sql
-- ride_events: 51K+ rows, filter by scooter + event type
ALTER TABLE urban_mobility_catalog.silver.ride_events CLUSTER BY (scooter_id, event_type);
OPTIMIZE urban_mobility_catalog.silver.ride_events;  -- 4 files → 1
```

### 10 — Unity Catalog Governance

**Column documentation** via `ALTER TABLE ... ALTER COLUMN ... COMMENT` on all Silver tables.

**Tags** for data governance:

| Tag | Columns |
|---|---|
| `business_key=true` | `trip_id`, `user_id`, `scooter_id`, `event_id` |
| `pii=true` | `user_id` (trips + users), `latitude`, `longitude` |
| `classification=confidential` | all PII columns |
| `sensitive=true` | `fare_mxn` |
| `domain=geolocation` | `latitude`, `longitude` |
| `foreign_key=users.user_id` | `trips.user_id` |

---

## Jobs Summary (7 active)

| Job | Type | Entity |
|---|---|---|
| UM — trips pipeline | Batch (4 tasks) | trips |
| UM — users pipeline | Batch (4 tasks) | users |
| UM — vehicles pipeline | Batch (4 tasks) | vehicles |
| UM — maintenance_logs pipeline | Batch (4 tasks) | maintenance_logs |
| UM — ride_events pipeline | Batch (4 tasks) | ride_events |
| UM — ride_events streaming pipeline | Streaming | ride_events |
| UM — referential integrity DQ | DQ cross-entity | All Silver |

All jobs: `demo_cluster` (`1117-181401-wlrft7z6`) · email alerts on failure.

---

## Roadmap

- [x] **Asset Bundles (DAB)** — multi-environment deployment (dev/prod) and CI/CD via GitHub Actions
- [x] **Extended DQ Dashboard** — metrics for all 5 entities with consolidated quarantine table
- [ ] **Integration tests** — validate Delta table contracts and row count expectations per run

---

## Author

**Alexis González**  
Data Engineer  
[LinkedIn](https://www.linkedin.com/in/alexis-gonzalez) · [GitHub](https://github.com/alexisantoniogonzalezharo)
