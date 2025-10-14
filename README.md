# 📊 Análisis de Efectos Adversos COVID-19 (VAERS)

**Autor: Valeria Chinchilla Mejías**

Pipeline completo de análisis de datos del sistema VAERS usando Apache Airflow, Spark, Druid y Superset.

## 🏗️ Arquitectura del Sistema

```
📁 Datos CSV VAERS → ⚡ Spark (ETL) → 🗄️ Druid → 📊 Superset → 👥 Usuarios
                           ↑
                   🚁 Airflow (orquestación)
```

## 📋 Análisis Implementados

### 1. **Síntomas Más Frecuentes por Fabricante** 🎯

- Top 10 síntomas reportados para PFIZER, MODERNA y JANSSEN
- Tasas de mortalidad y hospitalización por síntoma
- Edad promedio de pacientes por síntoma

### 2. **Análisis de Severidad por Edad** 👥

- Correlación entre edad y resultados severos
- Grupos: 0-17, 18-29, 30-49, 50-64, 65+
- Tasas de hospitalización, muerte y visitas ER

### 3. **Tiempo de Aparición de Síntomas** ⏰

- Tiempo promedio de aparición después de vacunación
- Distribución temporal por tipo de síntoma

### 4. **Distribución Geográfica** 🗺️

- Estados con mayor número de reportes
- Análisis regional de severidad

## 🚀 **INSTRUCCIONES DE USO**

### 1. **Generar datos de ejemplo**

```bash
python generate_sample_data.py
```

### 2. **Iniciar todos los servicios**

```bash
docker-compose up -d
```

### 3. **Ejecutar pipeline en Airflow**

- 🌐 **Airflow**: http://localhost:8080 (admin/admin)
- Activar DAG: `vaers_covid_analytics_pipeline`

### 4. **Ver dashboards en Superset**

- 🌐 **Superset**: http://localhost:8088 (admin/admin)

## 📊 **URLs de Servicios**

| Servicio        | URL                   | Credenciales |
| --------------- | --------------------- | ------------ |
| 🚁 **Airflow**  | http://localhost:8080 | admin/admin  |
| 📊 **Superset** | http://localhost:8088 | admin/admin  |
| 🗄️ **Druid**    | http://localhost:8888 | -            |
| ⚡ **Spark**    | http://localhost:8084 | -            |

## 🗂️ **Estructura de Archivos**

```
📁 adverse-effects-covid/
├── 📁 data/              # Datos CSV VAERS
├── 📁 dags/              # DAGs Airflow
├── 📁 spark/             # Scripts Spark
├── 📄 docker-compose.yml # Configuración servicios
├── 📄 generate_sample_data.py # Generador datos
└── 📄 README.md          # Esta documentación
```

¡Pipeline listo para analizar efectos adversos de vacunas COVID-19! 🚀
