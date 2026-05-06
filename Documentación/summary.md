# Urban Mobility Analytics — Resumen de Implementación

## Descripción General

Pipeline de Data Engineering end-to-end sobre **Databricks (Azure)** que simula la plataforma de datos de una empresa de e-scooter/bike sharing con operaciones en CDMX (50%), Monterrey (30%) y Guadalajara (20%). Arquitectura Medallion completa: Landing → Bronze → Silver → Gold.

---

## Stack Tecnológico

| Componente | Tecnología |
|---|---|
| Plataforma | Databricks on Azure |
| Storage | ADLS Gen2 (`abfss://demo@portfolioagdl.dfs.core.windows.net`) |
| Formato de tablas | Delta Lake |
| Catálogo | Unity Catalog — `urban_mobility_catalog` |
| Lenguaje | Python + PySpark |
| Orquestación | Databricks Jobs (4 jobs activos) |
| Configuración | YAML por entidad (`entity_config.yml` + `silver_rules.yml`) |

---

## Infraestructura en Unity Catalog

```
urban_mobility_catalog
├── bronze      (tablas Delta — datos raw + auditoría)
├── silver      (tablas Delta — datos limpios + DQ)
└── gold        (tablas Delta — agregaciones de negocio)
```

**Tablas creadas:**

| Tabla | Registros Bronze | Registros Silver | Pass Rate |
|---|---|---|---|
| `bronze/silver.trips` | 5,050 | 4,770 | 94.5% |
| `bronze/silver.users` | 500 | 500 | 100% |
| `bronze/silver.vehicles` | 200 | 200 | 100% |
| `bronze/silver.maintenance_logs` | 1,500 | 1,431 | 95.4% |
| `bronze/silver.ride_events` | 51,469 | 47,294 | 91.9% |

**Tablas auxiliares en Bronze:**
- `bronze.dq_run_results` — métricas de Data Quality por run
- `bronze.trips_quarantine` — registros rechazados de trips (138 registros)

---

## Estructura del Proyecto en Databricks

```
/Workspace/Users/alexis182@hotmail.com/urban_mobility_project/
├── helpers/
│   ├── metadata_framework     # Clases: MetaSchema, EntityConfig, SilverRulesConfig, DataProfiler
│   ├── transformations        # 44 transformaciones OOP + TransformationEngine (Registry Pattern)
│   ├── validations            # DQEngine — métricas a Delta Table
│   └── pipeline_base          # LayerProcessor (ABC), BronzeLoader, SilverCleaner, GoldAggregator
├── notebooks/
│   ├── 00_metadata_generator  # Infiere schema + genera YAMLs
│   ├── 01_bronze_loader       # Ingesta Landing → Bronze
│   ├── 02_silver_cleaner      # Limpieza + DQ Bronze → Silver
│   └── 03_gold_aggregator     # Agregaciones Silver → Gold
└── config/
    ├── trips/
    │   ├── entity_config.yml  # Configuración estructural + gold
    │   └── silver_rules.yml   # Reglas de transformación Silver
    ├── users/
    ├── vehicles/
    ├── maintenance_logs/
    └── ride_events/
```

---

## Arquitectura del Pipeline (Medallion)

```
ADLS Landing
    │  (CSV / JSON / Parquet)
    ▼
[00] Metadata Generator
    │  Infiere schema, perfila datos, genera YAMLs automáticamente
    │  entity_config.yml + silver_rules.yml
    ▼
[01] Bronze Loader  (BronzeLoader)
    │  Lee archivos del folder de entidad
    │  Agrega columnas de auditoría:
    │    _ingested_at, _source_file, _pipeline_run_id, _layer, _record_hash
    │  Escribe como Delta Table (overwrite)
    ▼
[02] Silver Cleaner  (SilverCleaner)
    │  Lee silver_rules.yml → aplica transformaciones dinámicas
    │  TransformationEngine (Registry Pattern) instancia clases por tipo
    │  Registros inválidos → tabla de cuarentena (_quarantine)
    │  Métricas por regla → dq_run_results
    │  Agrega columnas silver: _processing_timestamp, _dq_status, _dq_notes
    ▼
[03] Gold Aggregator  (GoldAggregator)
    │  Lee gold.aggregations de entity_config.yml
    │  Genera una tabla Delta independiente por cada agregación
    ▼
Gold Tables (Delta Lake)
```

---

## Diseño Orientado a Objetos (OOP)

### Patrones Implementados

| Patrón | Clase | Descripción |
|---|---|---|
| Template Method | `LayerProcessor` | `run()` orquesta fijo: `read → process → write` |
| Abstract Class | `LayerProcessor`, `BaseTransformation` | ABCs con métodos abstractos |
| Registry Pattern | `TransformationEngine.REGISTRY` | Dict de 44 tipos → clases Python |
| Composition (HAS-A) | `SilverCleaner` | Tiene un `TransformationEngine` y un `DQEngine` |
| Inheritance | `BronzeLoader`, `SilverCleaner`, `GoldAggregator` | Extienden `LayerProcessor` |
| Polymorphism | `apply(df)` | Cada transformación implementa su propia lógica |

### Jerarquía de Clases

```
LayerProcessor (ABC)
├── BronzeLoader
├── SilverCleaner
│   ├── HAS-A: TransformationEngine
│   └── HAS-A: DQEngine
└── GoldAggregator

BaseTransformation (ABC)
├── FilterTransformation (ABC)
│   ├── QuarantineNulls
│   ├── ClampNumeric
│   ├── ValidateEnum
│   ├── FixTimestampOrder
│   └── ... (otras con on_failure: drop | quarantine)
├── TrimStrings
├── NormalizeCaseTransformation
├── CleanCurrency
├── NormalizeDateFormat
├── DeduplicateTransformation
├── AddDerivedColumn
├── AddCategorization
└── ... (44 clases en total)
```

---

## Configuración YAML por Entidad

Cada entidad tiene **2 archivos de configuración**:

### `entity_config.yml` (configuración estructural)
```yaml
system:
  source_system: billing_legacy
  source_system_type: csv_export
  load_type: INCREMENTAL LOAD

entity:
  entity_name: trips
  file_type_name: csv
  primary_keys: [trip_id]
  landing_file_path: abfss://...

columns:
  - column_name: trip_id
    data_type: StringType
    nullable: false
    is_primary_key: true
    detected_format: high_cardinality

bronze:
  mode: overwrite

gold:
  aggregations:
    - name: trips_by_city_day
      group_by: [city]
      metrics: [...]
```

### `silver_rules.yml` (reglas de transformación)
```yaml
transformations:
  - type: trim_strings
  - type: quarantine_nulls
    columns: [trip_id, user_id]
    on_failure: quarantine
  - type: clean_currency
    column: fare_mxn
  - type: normalize_values
    column: city
    mapping: {cdmx: CDMX, MTY: Monterrey, ...}
  - type: fix_timestamp_order
    start_col: start_time
    end_col: end_time
    on_failure: quarantine
  - type: deduplicate
    keys: [trip_id]
  - type: detect_impossible_combination
    condition: "distance_km = 0 AND fare_mxn > 0"
    on_failure: flag
    flag_col: _invalid_fare_flag
  - type: add_derived_column
    output_col: duration_minutes
    expression: "(unix_timestamp(end_time) - unix_timestamp(start_time)) / 60"
  - type: add_audit_columns
    layer: silver

quality_checks: []
```

---

## DataProfiler — Auto-Detección de Transformaciones

El `DataProfiler` analiza los datos en landing y genera automáticamente el `silver_rules.yml`:

| Detección | Transformación Generada |
|---|---|
| Strings con `$` en numéricos | `clean_currency` |
| Duplicados en PK | `deduplicate` |
| Valores negativos en campos numéricos | `clamp_numeric` |
| Porcentajes > 100 | `clamp_numeric max_val=100` |
| Speed > 120 km/h | `clamp_numeric max_val=120` |
| Múltiples variantes de texto | `normalize_values` |
| Formatos de fecha mixtos (ISO + DD/MM/YYYY) | `normalize_date_format` |
| Columnas start/end con inversión de tiempo | `fix_timestamp_order` |

**Nota:** Columnas geográficas (`latitude`, `longitude`, etc.) están excluidas del clamp de negativos ya que los valores negativos son geográficamente válidos para México.

---

## Data Quality (DQ)

### Patrón de Cuarentena

Los registros inválidos no se descartan silenciosamente — van a tablas Delta independientes:

```
bronze.trips_quarantine    (registros rechazados de trips)
bronze.users_quarantine    (si aplica)
...
```

### Métricas en `bronze.dq_run_results`

```
run_id | entity_name | layer | rule | records_in | records_out | records_rejected | status
```

Cada ejecución del pipeline escribe métricas trazables por `pipeline_run_id = {entity}_{YYYYMMDD_HHMMSS}`.

---

## Dashboard de Data Quality

**URL:** `https://adb-335747394317880.0.azuredatabricks.net/sql/dashboardsv3/01f1483fea711aff928b4b2eb4c0182c`

**Nombre:** Urban Mobility — Data Quality Health

Contenido:
- 6 KPIs: Bronze Total, Silver Total, Cuarentena, Pass Rate %, Flagged Records, Tarifa Promedio MXN
- Gráfica: Flujo Bronze → Silver → Quarantine
- Gráfica: Distribución por Ciudad
- Gráfica: Categorías de Viaje (corto / medio / largo)
- Gráfica: Métodos de Pago
- Line chart: Tendencia mensual de viajes (18 meses)
- Tabla: Detalle de registros en cuarentena

---

## Jobs de Databricks (Activos)

| Job | Job ID | Entidad |
|---|---|---|
| UM — users pipeline | 519885520590792 | users |
| UM — vehicles pipeline | 1091570721590121 | vehicles |
| UM — maintenance_logs pipeline | 760764312505444 | maintenance_logs |
| UM — ride_events pipeline | 981127057927415 | ride_events |

Cada job tiene 4 tareas secuenciales: `t00_metadata → t01_bronze → t02_silver → t03_gold`.  
Todos usan el cluster `demo_cluster` (`1117-181401-wlrft7z6`). **Nunca serverless** (costo ~$60/semana en idle).

---

## Inconsistencias de Datos Capturadas

Las inconsistencias intencionales del dataset fueron manejadas por el pipeline:

| Entidad | Inconsistencia | Manejo |
|---|---|---|
| trips | `fare_mxn` como string `"$45.00"` | `clean_currency` → parse a Double |
| trips | `city` con variantes ("CDMX", "cdmx", "Ciudad de Mexico") | `normalize_values` → estandarizado |
| trips | `end_time < start_time` (timestamps invertidos) | `fix_timestamp_order` → cuarentena |
| trips | `user_id` nulo | `quarantine_nulls` → cuarentena |
| trips | Duplicados de `trip_id` (~1%) | `deduplicate` → eliminados |
| trips | `distance_km=0` con `fare_mxn>0` | `detect_impossible_combination` → flag `_invalid_fare_flag` |
| users | `plan_type` con variantes ("premium", "Premium", "PREMIUM") | `normalize_values` → "Premium" |
| users | `signup_date` en ISO y DD/MM/YYYY | `normalize_date_format` → `try_to_timestamp` null-safe |
| ride_events | `battery_pct` negativo o > 100 | `clamp_numeric min=0, max=100` |
| ride_events | `speed_kmh > 120` | `clamp_numeric max=120` |
| ride_events | Duplicados de `event_id` (~1%) | `deduplicate` |

---

## Mejoras de Arquitectura Implementadas (Fase 2)

### 1. MERGE/UPSERT para Cargas Incrementales

`LayerProcessor.write()` enruta automáticamente por `system.load_type`:

| load_type | Comportamiento |
|---|---|
| `INCREMENTAL LOAD` | MERGE Delta por PK — INSERT nuevos, UPDATE existentes |
| `FULL LOAD` | overwrite completo |
| `APPEND` | append sin modificar registros existentes |

`BronzeLoader` siempre sobreescribe (Bronze = réplica fiel incluyendo duplicados).

`_merge()` usa `col_map = {c: F.col("source.c") for c in df.columns}` en ambos `whenMatchedUpdate` y `whenNotMatchedInsert` para tolerancia a schema evolution.

### 2. Streaming Pipeline — ride_events

Notebook `04_streaming_ride_events`:

- **Auto Loader** (`cloudFiles.format: json`) lee landing folder incrementalmente
- Checkpoints en ADLS: `/Urban_Mobility/checkpoints/ride_events/{bronze,silver,schema}`
- `trigger(availableNow=True)` — procesa archivos pendientes y detiene el stream (compatible con Jobs)
- Bronze: append con audit columns + `_metadata.file_path`
- Silver: `foreachBatch` + MERGE por `event_id` (idempotente ante re-ejecuciones)
- Reutiliza `TransformationEngine` del pipeline batch para coherencia
- Job: `UM — ride_events streaming pipeline` (job_id: 697488533164639)

**Tablas creadas:**
- `bronze.ride_events_stream` — 51,469 registros
- `silver.ride_events_stream` — 47,294 registros (91.9% pass rate)

### 3. Integridad Referencial Cross-Entidad

Notebook `05_referential_integrity` valida FK entre tablas Silver:

| Relación | Total | Válidos | Huérfanos | Pass Rate | Status |
|---|---|---|---|---|---|
| `ride_events.trip_id → trips.trip_id` | 47,294 | 45,362 | 1,932 | 95.91% | WARN |
| `ride_events.scooter_id → vehicles.scooter_id` | 47,294 | 46,498 | 796 | 98.32% | WARN |
| `maintenance_logs.scooter_id → vehicles.scooter_id` | 1,431 | 1,354 | 77 | 94.62% | WARN |

Métricas persistidas en `bronze.dq_referential_integrity` con `run_id` trazable.
Job: `UM — referential integrity DQ` (job_id: 96601396579523)

### 4. Liquid Clustering

`CLUSTER BY` aplicado y `OPTIMIZE` ejecutado sobre tablas grandes:

| Tabla | Columnas de Clustering |
|---|---|
| `bronze.ride_events` | `scooter_id, event_type` |
| `silver.ride_events` | `scooter_id, event_type` (4 archivos → 1) |
| `bronze.trips` | `city, start_time` |
| `silver.trips` | `city, start_time` |
| `bronze.maintenance_logs` | `scooter_id, maintenance_date` |
| `silver.maintenance_logs` | `scooter_id, maintenance_date` |

### 5. Alertas en Databricks Jobs

`email_notifications.on_failure → alexisharo88@gmail.com` configurado en los 7 jobs:
trips, users, vehicles, maintenance_logs, ride_events (batch), ride_events (streaming), referential_integrity DQ.

### 6. Unity Catalog — Documentación y Tags

**Comentarios** en todas las tablas Silver (`COMMENT ON TABLE`) y columnas clave (`ALTER COLUMN ... COMMENT`).

**Tags aplicados (20 tags):**

| Tag | Columnas |
|---|---|
| `business_key=true` | `trip_id`, `user_id` (users), `scooter_id`, `event_id` |
| `pii=true` | `user_id` (trips + users), `latitude`, `longitude` |
| `classification=confidential` | `user_id` (trips + users), `latitude`, `longitude` |
| `sensitive=true` | `fare_mxn` |
| `domain=billing` | `fare_mxn` |
| `domain=geolocation` | `latitude`, `longitude` |
| `foreign_key=users.user_id` | `trips.user_id` |

---

## Jobs de Databricks (Activos — 7 total)

| Job | Job ID | Tipo | Entidad |
|---|---|---|---|
| UM — trips pipeline | 118392485379713 | Batch (4 tasks) | trips |
| UM — users pipeline | 519885520590792 | Batch (4 tasks) | users |
| UM — vehicles pipeline | 1091570721590121 | Batch (4 tasks) | vehicles |
| UM — maintenance_logs pipeline | 760764312505444 | Batch (4 tasks) | maintenance_logs |
| UM — ride_events pipeline | 981127057927415 | Batch (4 tasks) | ride_events |
| UM — ride_events streaming pipeline | 697488533164639 | Streaming | ride_events |
| UM — referential integrity DQ | 96601396579523 | DQ cross-entity | All Silver |

Todos usan `demo_cluster` (`1117-181401-wlrft7z6`). Todos con alertas de email en fallo.

---

## Próximos Pasos (Roadmap)

- [ ] **Fase 3 — Asset Bundles (DAB):** empaquetar el proyecto con CI/CD via GitHub Actions (diferido explícitamente)
- [ ] **Dashboard ampliado:** agregar métricas de las 4 entidades restantes al dashboard de DQ
- [ ] **Integration tests:** validar contratos Delta y row count expectations por run

---

*Última actualización: 2026-05-05*
