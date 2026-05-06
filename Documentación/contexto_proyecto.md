# Urban Mobility Analytics — Contexto del Proyecto

## Descripción general

Proyecto de portafolio profesional de Data Engineering llamado **Urban Mobility Analytics**.
Simula la plataforma de datos de una empresa de e-scooter / bike sharing con operaciones 
en múltiples ciudades de México.

El objetivo es demostrar una arquitectura de datos end-to-end production-grade sobre 
Databricks (Azure), combinando procesamiento batch y streaming.

## Stack tecnológico

- **Plataforma:** Databricks on Azure
- **Storage:** ADLS Gen2
- **Formato de tablas:** Delta Lake
- **Lenguaje:** Python + PySpark
- **Orquestación:** Databricks Jobs
- **CI/CD:** Databricks Asset Bundles (DAB) + GitHub Actions

## Entidades del dominio

| Entidad | Formato | Sistema origen |
|---|---|---|
| trips | CSV | Sistema legacy de facturación |
| users | JSON | API de registro de usuarios |
| vehicles | Parquet | Export del sistema de flota |
| maintenance_logs | CSV | Reporte de taller externo |
| ride_events | JSON Lines | Telemetría en tiempo real (streaming) |

## Inconsistencias intencionales en los datos

Estas inconsistencias están diseñadas para poner a prueba el pipeline de limpieza:

### trips (CSV)
- Algunos rows con user_id nulo
- Algunos rows con end_time anterior a start_time
- fare_mxn como string con símbolo de moneda: "$45.00" en ~10% de rows
- city con variantes inconsistentes: "CDMX", "cdmx", "Ciudad de Mexico", "Monterrey", "MTY", "Guadalajara", "GDL"
- Duplicados de trip_id (proporcional al volumen — aproximadamente 1%)
- distance_km = 0 con fare_mxn > 0 en algunos rows

### users (JSON)
- Algunos registros sin el campo email
- plan_type inconsistente: "premium", "Premium", "PREMIUM", "basic", "Basic"
- Algunos usuarios con city que no corresponde a ninguna ciudad operacional
- signup_date en formatos mixtos: ISO 8601 y "DD/MM/YYYY"

### vehicles (Parquet)
- Algunos scooter_id que NO aparecen en trips (vehículos huérfanos)
- Algunos scooter_id de trips que NO existen en vehicles (referencia rota)
- status con variantes: "active", "Active", "INACTIVE", "maintenance", null

### maintenance_logs (CSV)
- cost_usd con valores negativos en algunos rows
- maintenance_date con fechas futuras (errores de captura)
- Algunos scooter_id que no existen en vehicles

### ride_events (JSON Lines)
- battery_pct con valores negativos y mayores a 100
- speed_kmh nulo en algunos registros y mayor a 120 en otros (irreal para un scooter)
- Duplicados de event_id (aproximadamente 1%)
- trip_id que no existe en trips

## Estructura de carpetas

```
Portafolio/
├── Documentación/                  ← contexto y decisiones del proyecto
├── Fuentes_de_Datos/
│   └── raw/                        ← archivos fuente generados
│       ├── trips_sample.csv
│       ├── users_sample.json
│       ├── vehicles_sample.parquet
│       ├── maintenance_logs_sample.csv
│       └── ride_events_sample.json
└── Proceso_Medallion_Architecture/ ← código del pipeline
```

## Ciudades operacionales

| Ciudad | Distribución | Coordenadas aprox. |
|--------|-------------|-------------------|
| CDMX | 50% | 19.4326, -99.1332 |
| Monterrey | 30% | 25.6866, -100.3161 |
| Guadalajara | 20% | 20.6597, -103.3496 |
