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
| Capa | Propósito | Formato | Ubicación S3 |
|---|---|---|---|
| **Bronze** | Ingestión cruda sin transformaciones. Copia fiel del CSV original. | CSV | `s3://<bucket>/bronze/` |
| **Silver** | Limpieza, tipado, eliminación de nulos, estandarización de columnas. | Parquet | `s3://<bucket>/silver/` |
| **Gold** | Agregaciones de negocio: métricas por aerolínea, aeropuerto, rutas y temporalidad. | Parquet | `s3://<bucket>/gold/` |

### Flujo de Datos End-to-End
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

## ⚙️ Pipeline ETL

### Bronze — Ingestión (`etl/bronze.py`)

- Lee el archivo CSV original del dataset de vuelos.
- Lo sube tal cual a `s3://<bucket>/bronze/flights.csv`.
- No aplica ninguna transformación; preserva el dato crudo.
- Registra logs de tamaño del archivo y tiempo de carga.

### Silver — Transformación (`etl/silver.py`)

- Lee el CSV desde la capa Bronze en S3.
- Aplica las siguientes transformaciones:
  - Eliminación de filas con valores nulos críticos.
  - Conversión de tipos de dato (fechas, enteros, flotantes).
  - Estandarización de nombres de columnas (`snake_case`).
  - Eliminación de columnas redundantes o sin valor analítico.
- Escribe el resultado en formato **Parquet** en `s3://<bucket>/silver/flights.parquet`.

### Gold — Agregación (`etl/gold.py`)

- Lee los datos limpios desde la capa Silver.
- Genera tablas agregadas orientadas al negocio:
  - **Métricas por aerolínea**: total de vuelos, retrasos promedio, tasa de cancelación.
  - **Métricas por aeropuerto**: tráfico de origen/destino, retrasos promedio.
  - **Métricas temporales**: tendencias mensuales, día de la semana.
  - **Métricas por ruta**: pares origen-destino más frecuentes.
- Escribe cada tabla en formato **Parquet** en `s3://<bucket>/gold/`.

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
