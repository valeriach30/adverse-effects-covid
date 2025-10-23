# Análisis de Efectos Adversos COVID-19 (VAERS)

**Autor: Valeria Chinchilla Mejías**

## 📊 Arquitectura del Pipeline

```
📊 CSV → 🐻‍❄️ Polars ETL → 🐲 Druid + 🗄️ PostgreSQL → 📈 Superset
```

Este proyecto implementa un pipeline de análisis de datos VAERS (Vaccine Adverse Event Reporting System) para efectos adversos de vacunas COVID-19.

## 🏗️ Arquitectura Completa

### Componentes principales

1. **📂 Datos**: Archivos CSV de VAERS (1.1 GB total)
2. **🐻‍❄️ Polars ETL**: Procesamiento eficiente de datos
3. **🌊 Apache Airflow**: Orquestación del pipeline
4. **🐲 Apache Druid**: Base de datos analítica para consultas rápidas
5. **🗄️ PostgreSQL**: Base de datos relacional para Superset
6. **📈 Apache Superset**: Dashboards y visualizaciones

#### 🚀 ¿Por qué Polars en lugar de Spark?

##### ✅ Ventajas de Polars para este proyecto

- **Tamaño del dataset**: 1.1 GB es perfecto para Polars (puede manejar mucho más)
- **Simplicidad**: API más intuitiva y fácil de debuggear
- **Performance**: Extremadamente rápido para datasets de este tamaño
- **Recursos**: No necesita un cluster completo como Spark
- **Memoria**: Uso más eficiente de memoria RAM
- **Configuración**: Setup mucho más simple

##### ❌ Problemas que se evitan con Spark

- Complejidad de configuración de cluster
- Overhead innecesario para datasets < 10GB
- Configuración compleja de memoria y cores
- Dependencias Java pesadas
- Debugging más complicado

### Flujo de datos

```
VAERSDATA.csv (919MB)     ─┐
VAERSSYMPTOMS.csv (105MB) ─┤─► Polars ETL ─► JSON ─► Druid
VAERSVAX.csv (80MB)       ─┘                 └─► CSV ─► PostgreSQL ─► Superset
```

## 📁 Estructura del Proyecto

```
adverse-effects-covid/
├── 📊 data/                          # Datos CSV de VAERS
│   ├── VAERSDATA.csv                 # Datos principales (919MB)
│   ├── VAERSSYMPTOMS.csv            # Síntomas (105MB)
│   └── VAERSVAX.csv                 # Vacunas (80MB)
├── 🐻‍❄️ etl/
│   └── etl_processor.py             # ETL principal con Polars (chunked)
├── 🌊 dags/
│   └── main_pipeline.py             # DAG principal del pipeline
├── 📈 superset/                      # Scripts de Superset
│   ├── dashboard_setup.py           # Configuración automática de dashboards
│   ├── dataset_manager.py           # Gestión de datasets
│   └── dashboard_validator.py       # Validación de dashboards
├── 🐳 Docker:
│   ├── docker-compose.yml           # Configuración Docker principal
│   └── requirements-polars.txt      # Dependencias de Polars
└── 📚 Documentación:
    ├── README.md                    # Este archivo
```

## 🚀 Correr el proyecto

### 1. Iniciar docker

```bash
# Iniciar todos los servicios
docker compose up -d
```

### 2. Acceder a los servicios

- **🌊 Airflow**: <http://localhost:8080> (admin/admin)
- **🐲 Druid**: <http://localhost:8888>
- **📈 Superset**: <http://localhost:8088> (admin/admin)

## 📊 Charts Generados

El pipeline genera 4 tipos de análisis:

### 1. 🔬 Reportes por Fabricante de Vacuna

- **Objetivo**: Identificar la cantidad de reportes por marca de vacuna
- **Métricas**: Total reportes, casos únicos, muertes, hospitalizaciones, tasas
- **Archivo**: `symptoms_for_druid.json`, `symptoms_analysis.csv`

### 2. Top 15 Síntomas más reportados

- **Objetivo**: Analizar los síntomas más reportados
- **Métricas**: Casos totales, muertes, hospitalizaciones, tasas de severidad
- **Archivo**: `symptoms_for_druid.json`, `symptoms_analysis.csv`

### 3. 📈 Hospitalizaciones por Grupo de Edad

- **Objetivo**: Analizar severidad de efectos adversos por edad
- **Grupos**: 0-17, 18-29, 30-49, 50-64, 65+
- **Métricas**: Casos totales, muertes, hospitalizaciones, tasas de severidad
- **Archivo**: `severity_for_druid.json`, `severity_analysis.csv`

### 4. 🗺️ Distribución Geográfica por Estado

- **Objetivo**: Análisis por estado
- **Métricas**: Reportes totales, muertes, hospitalizaciones por estado
- **Archivo**: `geographic_for_druid.json`, `geographic_analysis.csv`

## 🐳 Servicios Docker

### Servicios principales

- **volume-init**: Inicializa volúmenes compartidos
- **postgres**: Base de datos (Airflow + Superset + Druid metadata)
- **airflow-webserver/scheduler**: Orquestación
- **zookeeper**: Coordinación para Druid
- **druid-coordinator/broker/historical/middlemanager/router**: Cluster Druid
- **superset**: Dashboards y visualización
- **monitor**: Monitoreo del sistema

### Volúmenes compartidos

- **shared_data**: Intercambio de datos entre servicios
- **metadata_data**: Base de datos PostgreSQL
- **druid_shared**: Datos compartidos de Druid

## 🎯 Estado del Proyecto

### ✅ Completado

- **Pipeline completo funcionando** con Polars chunked ETL
- **Arquitectura optimizada** con procesamiento por chunks (100k filas)
- **Dashboards automáticos** con filtros de calidad de datos
- **Limpieza de código** y estructura organizada
- **Datos filtrados** solo fabricantes principales (PFIZER, MODERNA, JANSSEN)
- **Estados geográficos válidos** sin datos "US" genéricos

### 🎯 Métricas del Pipeline

- **Procesamiento**: ~193 chunks en ~60 segundos
- **Datos procesados**: 1.1GB → análisis filtrados
- **Fabricantes**: 3 principales (PFIZER: 7149, MODERNA: 6589, JANSSEN: 3621 reportes)
- **Estados**: 15 principales estados de EE.UU.
- **Automatización completa**: DAG → ETL → Druid → Superset

### 🔄 Optimizaciones Implementadas

- **Chunked processing** para manejo eficiente de memoria
- **Filtros de calidad** para síntomas y fabricantes
- **Datos geográficos realistas** con distribución por estados
- **Validación automática** de datasets en Superset
- **Nomenclatura simplificada** de archivos y DAGs
