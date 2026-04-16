# ✈️ flights-data-engineering-a

**End-to-End Data Engineering Pipeline — U.S. Domestic Flights 2015**

> Tarea 08 · Maestría en Ciencia de Datos · ITAM 2026

---

## 👥 Autores

- José Antonio Esparza
- Gustavo Pardo 

---

## 📋 Descripción General

Este proyecto implementa un pipeline de datos **end-to-end** sobre el dataset de vuelos domésticos de Estados Unidos del año 2015, que contiene aproximadamente **5.8 millones de registros**. El objetivo es demostrar competencias profesionales en ingeniería de datos: ingestión, transformación, almacenamiento, modelado relacional y análisis avanzado.

El diseño sigue la **arquitectura Medallion** (Bronze → Silver → Gold) sobre Amazon S3, con catalogación en AWS Glue, consultas analíticas en Amazon Athena, persistencia relacional en PostgreSQL (Amazon RDS) y un análisis exploratorio y predictivo en Jupyter Notebook.

| Aspecto | Detalle |
|---|---|
| **Dataset** | U.S. Domestic Flights 2015 (~5.8 M registros) |
| **Arquitectura** | Medallion (Bronze / Silver / Gold) |
| **Almacenamiento** | Amazon S3 + Parquet |
| **Catálogo** | AWS Glue Data Catalog |
| **Consultas** | Amazon Athena |
| **Base de datos** | PostgreSQL en Amazon RDS |
| **Análisis** | Jupyter Notebook (pandas, scikit-learn, statsmodels) |

---

## 🏗️ Arquitectura del Proyecto

### Arquitectura Medallion

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                          MEDALLION ARCHITECTURE                                 │
│                                                                                 │
│  ┌──────────────┐      ┌──────────────┐      ┌──────────────┐                  │
│  │              │      │              │      │              │                  │
│  │   🥉 BRONZE  │─────▶│   🥈 SILVER  │─────▶│   🥇 GOLD    │                  │
│  │              │      │              │      │              │                  │
│  └──────┬───────┘      └──────┬───────┘      └──────┬───────┘                  │
│         │                     │                     │                          │
│    Raw Ingestion         Aggregations          Denormalized                    │
│    CSV → Parquet         Business KPIs         Analytical Table                │
│                                                                                 │
│  ┌──────────────────────────────────────────────────────────────────────────┐   │
│  │                        AWS SERVICES                                      │   │
│  │  S3 (Storage)  ·  Glue (Catalog)  ·  Athena (Query)  ·  RDS (RDBMS)    │   │
│  └──────────────────────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────────────────┘
```

#### 🥉 Bronze — Ingestión Cruda

| Aspecto | Detalle |
|---|---|
| **Propósito** | Copia fiel del dataset original sin transformaciones |
| **Formato** | Parquet (convertido desde CSV) |
| **Tablas** | `flights` · `airlines` · `airports` |
| **S3** | `s3://<bucket>/flights/bronze/{tabla}/` |
| **Glue DB** | `flights_bronze` |
| **Script** | `etl/bronze.py` |

```
CSV (local)  ──▶  Validate  ──▶  Parquet (S3)  ──▶  Glue Catalog
```

#### 🥈 Silver — Agregaciones de Negocio

| Aspecto | Detalle |
|---|---|
| **Propósito** | Métricas agregadas por día, mes/aerolínea y aeropuerto |
| **Formato** | Parquet + Snappy |
| **S3** | `s3://<bucket>/flights/silver/{tabla}/` |
| **Glue DB** | `flights_silver` |
| **Script** | `etl/silver.py` |

| Tabla Silver | Agrupación | Métricas |
|---|---|---|
| `flights_daily` | YEAR, MONTH, DAY | total_flights, total_delayed, total_cancelled, avg_departure_delay, avg_arrival_delay |
| `flights_monthly` | MONTH, AIRLINE | total_flights, total_delayed, total_cancelled, avg_arrival_delay, on_time_pct |
| `flights_by_airport` | ORIGIN_AIRPORT | total_departures, total_delayed, total_cancelled, avg_departure_delay, pct_weather_delay |

```
Bronze (S3 Parquet)  ──file-by-file──▶  Partial Aggs  ──combine──▶  Silver (S3 + Glue)
```

#### 🥇 Gold — Tabla Analítica Desnormalizada

| Aspecto | Detalle |
|---|---|
| **Propósito** | Vista desnormalizada lista para consumo analítico |
| **Método** | CTAS ejecutado en Athena (JOINs sobre Bronze) |
| **Tabla** | `vuelos_analitica` (~5.8M filas, 20 columnas) |
| **S3** | `s3://<bucket>/flights/gold/` |
| **Glue DB** | `flights_gold` |
| **Script** | `etl/gold.py` |

```
Bronze tables (Glue)  ──CTAS (Athena)──▶  Gold analytical table (S3 + Glue)

  flights ──┐
  airlines ─┤── LEFT JOINs ──▶  vuelos_analitica
  airports ─┘
```

> **Nota:** El CTAS se construye sobre Bronze para demostrar que Gold puede
> construirse desde cualquier capa. En producción se construiría desde Silver
> para aprovechar el Parquet ya procesado.

#### Flujo End-to-End

```
  download_data.sh          bronze.py              silver.py               gold.py
  ┌──────────┐        ┌──────────────┐       ┌──────────────┐       ┌──────────────┐
  │ Download │──CSV──▶│  Ingest Raw  │──S3──▶│  Aggregate   │──S3──▶│  CTAS Join   │
  │   CSVs   │        │  to Parquet  │       │  KPIs        │       │  Denormalize │
  └──────────┘        └──────┬───────┘       └──────┬───────┘       └──────┬───────┘
                             │                      │                      │
                        Glue: flights_bronze   Glue: flights_silver   Glue: flights_gold
                        ├─ flights             ├─ flights_daily       └─ vuelos_analitica
                        ├─ airlines            ├─ flights_monthly
                        └─ airports            └─ flights_by_airport
```

---

## 🛠️ Tecnologías Utilizadas

| Categoría | Herramientas |
|---|---|
| **Lenguaje** | Python 3.10+ |
| **Cloud** | AWS (S3, Glue, Athena, RDS) |
| **Almacenamiento** | Amazon S3 (Parquet, CSV) |
| **Catálogo** | AWS Glue Data Catalog |
| **Consultas** | Amazon Athena |
| **Base de datos** | PostgreSQL 15 (Amazon RDS) |
| **ETL** | boto3, pandas, pyarrow |
| **Análisis** | pandas, matplotlib, seaborn |
| **Modelado** | scikit-learn, statsmodels |
| **Infraestructura** | CloudFormation (YAML) |
| **Notebook** | Jupyter / JupyterLab |
| **Control de versiones** | Git + GitHub |

---

## 📁 Estructura del Repositorio
```
lights-data-engineering-a/
├── artifacts/ → Screenshots de evidencia para evaluación académica
│ └── screenshots/
├── config/ → Variables AWS, credenciales y conexiones PostgreSQL
│ └── init.py
├── db/ → DDL scripts y esquemas del modelo relacional
├── docs/ → Diagrama ER y documentación técnica
│ ├── erd-flights.drawio
│ ├── erd-flights.png
│ └── screenshots/
├── etl/ → Pipeline Medallion (Bronze → Silver → Gold)
│ ├── bronze.py
│ ├── gold.py
│ └── silver.py
├── infra/ → CloudFormation templates para RDS PostgreSQL
│ └── rds-flights.yaml
├── notebooks/ → EDA, regresión lineal y forecasting
│ └── flights_analytics.ipynb
├── README.md
├── requirements.txt
├── sql/ → CREATE TABLE statements y analytical queries
├── src/ → Utilities: S3 client, PostgreSQL connector, validators
│ └── init.py
└── tests/ → Unit tests y data quality checks
└── init.py
```

---

## ⚙️ Pipeline ETL

### Bronze — `etl/bronze.py`

```bash
python etl/bronze.py --bucket <tu-bucket> --data-dir data/flights/
```

- Lee los 3 archivos CSV originales (`flights.csv`, `airlines.csv`, `airports.csv`).
- Convierte a Parquet y sube a `s3://<bucket>/flights/bronze/{tabla}/`.
- `flights.csv` (~5.8M filas) se procesa en chunks de 500K filas para evitar OOM.
- Registra las tablas en Glue Data Catalog (`flights_bronze`).

### Silver — `etl/silver.py`

```bash
python etl/silver.py --bucket <tu-bucket>
```

- Lee Bronze archivo por archivo (sin Ray) con solo las columnas necesarias.
- Computa agregaciones parciales por chunk y las combina al final.
- Genera 3 tablas Silver: `flights_daily`, `flights_monthly`, `flights_by_airport`.
- `flights_daily` particionada por `MONTH` (`overwrite_partitions`).
- Todas registradas en Glue Data Catalog (`flights_silver`).

### Gold — `etl/gold.py`

```bash
python etl/gold.py --bucket <tu-bucket>
```

- Verifica que las tablas Bronze existan en Glue.
- Ejecuta un CTAS en Athena que une `flights` + `airlines` + `airports`.
- Genera la tabla desnormalizada `vuelos_analitica` (20 columnas).
- Valida que `airline_name`, `origin_airport_name` y `destination_airport_name` estén poblados.

---

### Buenas Prácticas Implementadas

| Práctica | Descripción |
|---|---|
| **Idempotencia** | Cada script puede ejecutarse múltiples veces sin duplicar datos (sobrescritura controlada). |
| **Logging** | Uso de `logging` para registrar progreso, errores y métricas de ejecución. |
| **Modularidad** | Funciones atómicas y reutilizables en `src/`. Cada capa es independiente. |
| **Separación de configuración** | Parámetros externalizados en `config/settings.py`. |

---

## 🗄️ Modelo Relacional (PostgreSQL)

El modelo relacional está diseñado en **tercera forma normal (3NF)** y desplegado en una instancia de Amazon RDS PostgreSQL.

### Tablas Principales

| Tabla | Descripción |
|---|---|
| `airlines` | Catálogo de aerolíneas (código IATA, nombre). |
| `airports` | Catálogo de aeropuertos (código IATA, ciudad, estado, latitud, longitud). |
| `flights` | Tabla de hechos: cada vuelo con sus atributos operativos. |
| `delays` | Detalle de retrasos por tipo (clima, seguridad, aerolínea, NAS, avión tardío). |
| `cancellations` | Registro de cancelaciones con motivo codificado. |

### Diagrama Entidad-Relación

El diagrama ER se encuentra en:

- **Imagen**: [docs/erd-flights.png](docs/erd-flights.png)
- **Editable**: [docs/erd-flights.drawio](docs/erd-flights.drawio)

### Infraestructura

La instancia de PostgreSQL se aprovisiona mediante CloudFormation:

```bash
aws cloudformation deploy \
  --template-file infra/rds-flights.yaml \
  --stack-name flights-rds-stack \
  --parameter-overrides DBPassword=<your-password>
```

**Nota importante**: 

- `chmod +x download_data.sh` para dar permisos de ejecución
- `./download_data.sh` para ejecutar el script

Esta sección debe ser añadida después de la última línea del bloque CloudFormation de la sección "Infraestructura".

---

## Scripts de Ejecución

| Script | Responsabilidad |
|--------|----------------|
| `etl/bronze.py` | Sube los tres CSVs a S3 y los registra en Glue |
| `etl/silver.py` | Transforma a Parquet + Snappy y construye las tres agregaciones |
| `etl/gold.py` | Ejecuta el CTAS en Athena para construir la tabla analítica |

### Ejecución desde Terminal

Ejecuta los scripts en orden desde el terminal de SageMaker Jupyter Lab:

```bash
python etl/bronze.py --bucket <tu-bucket> --data-dir data/flights/
python etl/silver.py --bucket <tu-bucket>
python etl/gold.py --bucket <tu-bucket>
````
### 3. Crear entorno virtual e instalar dependencias

```bash
python -m venv .venv
source .venv/bin/activate  # En Linux/Mac
# .venv\Scripts\activate   # En Windows

pip install --upgrade pip
pip install -r requirements.txt
````
