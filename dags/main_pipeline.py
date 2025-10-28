#!/usr/bin/env python3
"""
DAG de Airflow para procesar datos VAERS con Polars
📊 CSV → 🐻‍❄️ Polars ETL → 🐲 Druid + 🗄️ PostgreSQL → 📈 Superset
"""

# Standard library imports
import json
import logging
import os
import subprocess
import sys
import time
from datetime import datetime, timedelta

# Third-party imports
import pandas as pd
import polars as pl
import requests
from sqlalchemy import create_engine

# Airflow imports
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator

# Local imports
sys.path.append('/opt/airflow/etl')
from etl_processor import run_chunked_pipeline

# Configuración por defecto del DAG
default_args = {
    'owner': 'vaers-polars-team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'catchup': False
}

# Definir el DAG
dag = DAG(
    'main_pipeline',
    default_args=default_args,
    description='Pipeline VAERS con Polars: CSV → Polars ETL → Druid + PostgreSQL → Superset',
    schedule_interval=None,  # Ejecutar solo manualmente
    max_active_runs=1,
    tags=['vaers', 'covid', 'polars', 'druid', 'superset']
)

# Función para preparar el entorno
def setup_shared_directories():
    """Verificar que el volumen compartido esté disponible"""

    directories = ['/opt/shared_data/vaers_results', '/opt/shared_data/druid_ingestion_specs']

    for directory in directories:
        if not os.path.exists(directory):
            print(f"⚠️ Directorio no existe, creándolo: {directory}")
            os.makedirs(directory, mode=0o777, exist_ok=True)

        # Verificar que se puede escribir
        test_file = os.path.join(directory, 'test_write.tmp')
        with open(test_file, 'w') as f:
            f.write('test')
        os.remove(test_file)
        print(f"✅ Directorio funcional: {directory}")

    return "Volumen compartido verificado correctamente"

# Función para verificar datos
def check_data_quality():
    """Verifica la calidad de los datos de entrada"""

    data_path = "/opt/airflow/data"
    required_files = ["VAERSDATA.csv", "VAERSSYMPTOMS.csv", "VAERSVAX.csv"]

    for file in required_files:
        file_path = f"{data_path}/{file}"
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"Archivo requerido no encontrado: {file}")

        # Verificar que el archivo no esté vacío usando Polars
        try:
            df = pl.read_csv(file_path, n_rows=5)
            if df.height == 0:
                raise ValueError(f"Archivo vacío: {file}")
            print(f"✅ {file} verificado - {df.height} filas de muestra, {df.width} columnas")
        except Exception as e:
            print(f"❌ Error verificando {file}: {e}")
            raise

    return "Verificación de datos completada exitosamente"

# Función adicional para verificar que los archivos existan (reemplaza FileSensor)
def check_required_files():
    """Verifica que todos los archivos requeridos existan"""

    data_path = "/opt/airflow/data"
    required_files = ["VAERSDATA.csv", "VAERSSYMPTOMS.csv", "VAERSVAX.csv"]

    print(f"🔍 Verificando archivos en {data_path}...")

    for file in required_files:
        file_path = f"{data_path}/{file}"
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"❌ Archivo requerido no encontrado: {file_path}")

        # Verificar tamaño del archivo
        size = os.path.getsize(file_path)
        size_mb = size / 1024 / 1024
        print(f"✅ {file}: {size_mb:.1f} MB")
    print("✅ Todos los archivos requeridos están disponibles")
    return "Archivos verificados exitosamente"

# Función para ejecutar ETL con Polars
def run_polars_etl():
    """Ejecutar ETL de VAERS usando Polars chunked (OPTIMIZADO PARA TODO EL DATASET)"""
    
    logger = logging.getLogger(__name__)

    try:
        logger.info("🚀 Iniciando ETL VAERS Polars chunked COMPLETO...")
        result = run_chunked_pipeline()
        logger.info("✅ ETL completado: %s", result)
        return result
    except Exception as e:
        logger.error("❌ Error en ETL: %s", str(e))
        raise

# Función para preparar datos para Druid
def prepare_druid_ingestion():
    """Prepara archivos de especificación de ingesta para Druid"""

    spec_path = "/opt/shared_data/druid_ingestion_specs"
    data_path = "/opt/shared_data/vaers_results"

    # Verificar que los directorios existen
    if not os.path.exists(spec_path):
        raise Exception(f"❌ Directorio de specs no existe: {spec_path}")
    if not os.path.exists(data_path):
        raise Exception(f"❌ Directorio de datos no existe: {data_path}")

    print(f"✅ Usando volumen compartido para Druid: {spec_path}")

    # 1. Especificación para síntomas por fabricante (CHUNKED)
    symptoms_spec = {
        "type": "index_parallel",
        "spec": {
            "ioConfig": {
                "type": "index_parallel",
                "inputSource": {
                    "type": "local",
                    "baseDir": "/opt/shared_data/vaers_results",
                    "filter": "chunked_symptoms_for_druid.json"
                },
                "inputFormat": {"type": "json"}
            },
            "tuningConfig": {
                "type": "index_parallel",
                "partitionsSpec": {"type": "dynamic"}
            },
            "dataSchema": {
                "dataSource": "vaers_symptoms_by_manufacturer",
                "timestampSpec": {"column": "__time", "format": "iso"},
                "dimensionsSpec": {"dimensions": ["manufacturer", "symptom"]},
                "metricsSpec": [
                    {"name": "total_reports", "type": "longSum", "fieldName": "total_reports"},
                    {"name": "deaths", "type": "longSum", "fieldName": "deaths"},
                    {"name": "hospitalizations", "type": "longSum", "fieldName": "hospitalizations"}
                ],
                "granularitySpec": {"type": "uniform", "segmentGranularity": "DAY", "queryGranularity": "NONE"}
            }
        }
    }

    # 2. Especificación para severidad por edad
    severity_spec = {
        "type": "index_parallel",
        "spec": {
            "ioConfig": {
                "type": "index_parallel",
                "inputSource": {
                    "type": "local",
                    "baseDir": "/opt/shared_data/vaers_results",
                    "filter": "severity_for_druid.json"
                },
                "inputFormat": {"type": "json"}
            },
            "tuningConfig": {"type": "index_parallel", "partitionsSpec": {"type": "dynamic"}},
            "dataSchema": {
                "dataSource": "vaers_severity_by_age",
                "timestampSpec": {"column": "__time", "format": "iso"},
                "dimensionsSpec": {"dimensions": ["age_group", "VAX_MANU_CLEAN"]},
                "metricsSpec": [
                    {"name": "total_reports", "type": "longSum", "fieldName": "total_cases"},
                    {"name": "deaths", "type": "longSum", "fieldName": "deaths"},
                    {"name": "hospitalizations", "type": "longSum", "fieldName": "hospitalizations"},
                    {"name": "er_visits", "type": "longSum", "fieldName": "er_visits"},
                    {"name": "severe_cases", "type": "longSum", "fieldName": "severe_cases"},
                    {"name": "death_rate", "type": "doubleSum", "fieldName": "death_rate"},
                    {"name": "hospital_rate", "type": "doubleSum", "fieldName": "hospital_rate"},
                    {"name": "severe_rate", "type": "doubleSum", "fieldName": "severe_rate"},
                    {"name": "avg_age", "type": "doubleSum", "fieldName": "avg_age"}
                ],
                "granularitySpec": {"type": "uniform", "segmentGranularity": "DAY", "queryGranularity": "NONE"}
            }
        }
    }

    # 3. Especificación para distribución geográfica
    geographic_spec = {
        "type": "index_parallel",
        "spec": {
            "ioConfig": {
                "type": "index_parallel",
                "inputSource": {
                    "type": "local",
                    "baseDir": "/opt/shared_data/vaers_results",
                    "filter": "geographic_for_druid.json"
                },
                "inputFormat": {"type": "json"}
            },
            "tuningConfig": {"type": "index_parallel", "partitionsSpec": {"type": "dynamic"}},
            "dataSchema": {
                "dataSource": "vaers_geographic_distribution",
                "timestampSpec": {"column": "__time", "format": "iso"},
                "dimensionsSpec": {"dimensions": ["state", "VAX_MANU_CLEAN"]},
                "metricsSpec": [
                    {"name": "total_reports", "type": "longSum", "fieldName": "total_reports"},
                    {"name": "deaths", "type": "longSum", "fieldName": "deaths"},
                    {"name": "hospitalizations", "type": "longSum", "fieldName": "hospitalizations"},
                    {"name": "er_visits", "type": "longSum", "fieldName": "er_visits"},
                    {"name": "avg_age", "type": "doubleSum", "fieldName": "avg_age"},
                    {"name": "death_rate", "type": "doubleSum", "fieldName": "death_rate"},
                    {"name": "hospital_rate", "type": "doubleSum", "fieldName": "hospital_rate"}
                ],
                "granularitySpec": {"type": "uniform", "segmentGranularity": "DAY", "queryGranularity": "NONE"}
            }
        }
    }

    # Guardar especificaciones
    specs = {
        "symptoms_ingestion.json": symptoms_spec,
        "severity_ingestion.json": severity_spec,
        "geographic_ingestion.json": geographic_spec
    }

    for filename, spec in specs.items():
        spec_file = os.path.join(spec_path, filename)
        with open(spec_file, 'w') as f:
            json.dump(spec, f, indent=2)
        print(f"✅ Especificación creada: {spec_file}")

    return f"Especificaciones de Druid creadas en {spec_path}"

# Función para cargar datos a PostgreSQL
def load_to_postgresql():
    """Carga los datos procesados a PostgreSQL para Superset"""

    # Conexión a PostgreSQL
    engine = create_engine('postgresql://superset:superset@postgres:5432/superset')

    # Archivos chunked a cargar
    files_to_load = [
        ("chunked_symptoms_analysis.csv", "vaers_symptoms_analysis"),
        ("severity_analysis.csv", "vaers_severity_analysis"),
        ("geographic_analysis.csv", "vaers_geographic_analysis")
    ]

    data_path = "/opt/shared_data/vaers_results"

    for csv_file, table_name in files_to_load:
        file_path = f"{data_path}/{csv_file}"

        if os.path.exists(file_path):
            try:
                # Leer CSV
                df = pd.read_csv(file_path)

                # Cargar a PostgreSQL
                df.to_sql(table_name, engine, if_exists='replace', index=False)
                print(f"✅ Tabla {table_name} cargada con {len(df)} filas")

            except Exception as e:
                print(f"❌ Error cargando {table_name}: {e}")
                raise
        else:
            print(f"⚠️ Archivo no encontrado: {file_path}")

    return "Datos cargados a PostgreSQL exitosamente"

# Función para refrescar datasets en Superset
def refresh_superset_datasets():
    """Refresca los datasets en Superset para sincronizar columnas con Druid"""

    print("🔄 Refrescando datasets en Superset...")
    time.sleep(10)  # Esperar a que Druid termine

    try:
        # Ejecutar script de refresh
        result = subprocess.run([
            'python', '/opt/airflow/superset/dataset_manager.py'
        ], capture_output=True, text=True, timeout=120)

        if result.returncode == 0:
            print("✅ Datasets refrescados exitosamente")
            print(result.stdout)
            return "Datasets refrescados exitosamente"
        else:
            print(f"❌ Error refrescando datasets: {result.stderr}")
            # No fallar el pipeline por esto
            return "Warning: Error refrescando datasets"

    except Exception as e:
        print(f"❌ Error ejecutando refresh: {e}")
        return "Warning: Error en refresh de datasets"

# Función para verificar conectividad con Druid
def check_druid_connectivity():
    """Verificar que Druid esté disponible antes de intentar ingesta"""
    import requests
    
    druid_url = "http://router:8888/status"
    max_retries = 10
    retry_delay = 30
    
    print("🔍 Verificando conectividad con Druid...")
    
    for attempt in range(max_retries):
        try:
            response = requests.get(druid_url, timeout=10)
            if response.status_code == 200:
                print(f"✅ Druid disponible en {druid_url}")
                return "Druid conectado exitosamente"
        except Exception as e:
            print(f"❌ Intento {attempt + 1}/{max_retries} falló: {e}")
        
        if attempt < max_retries - 1:
            print(f"⏳ Esperando {retry_delay}s antes del siguiente intento...")
            time.sleep(retry_delay)
    
    raise Exception("❌ No se pudo conectar con Druid después de múltiples intentos")

# Función para configurar Superset
def setup_superset_dashboards():
    """Configura los dashboards en Superset"""

    # Esperar a que Superset esté disponible
    print("⏳ Esperando a que Superset esté disponible...")
    time.sleep(30)

    try:
        # Ejecutar script de configuración de dashboard
        result = subprocess.run([
            'python', '/opt/airflow/superset/dashboard_setup.py'
        ], capture_output=True, text=True, timeout=300)

        if result.returncode == 0:
            print("✅ Dashboards de Superset configurados exitosamente")
            print(result.stdout)
            return "Dashboards configurados exitosamente"
        else:
            print(f"❌ Error configurando dashboards: {result.stderr}")
            raise Exception(f"Error en configuración de Superset: {result.stderr}")

    except subprocess.TimeoutExpired:
        print("⏰ Timeout configurando Superset")
        raise
    except Exception as e:
        print(f"❌ Error ejecutando configuración de Superset: {e}")
        raise

# ===== DEFINICIÓN DE TAREAS =====

# 1. Verificar que los datos existan
check_data_task = PythonOperator(
    task_id='check_data_files',
    python_callable=check_required_files,
    dag=dag
)

# 2. Configurar directorios compartidos
setup_directories_task = PythonOperator(
    task_id='setup_shared_directories',
    python_callable=setup_shared_directories,
    dag=dag
)

# 3. Verificar calidad de datos
quality_check_task = PythonOperator(
    task_id='data_quality_check',
    python_callable=check_data_quality,
    dag=dag
)

# 4. Ejecutar ETL con Polars
polars_etl_task = PythonOperator(
    task_id='run_polars_etl',
    python_callable=run_polars_etl,
    dag=dag
)

# 5. Verificar conectividad con Druid
druid_connectivity_task = PythonOperator(
    task_id='check_druid_connectivity',
    python_callable=check_druid_connectivity,
    dag=dag
)

# 6. Preparar especificaciones de Druid
druid_prep_task = PythonOperator(
    task_id='prepare_druid_ingestion',
    python_callable=prepare_druid_ingestion,
    dag=dag
)

# 7. Cargar datos a PostgreSQL
postgres_load_task = PythonOperator(
    task_id='load_to_postgresql',
    python_callable=load_to_postgresql,
    dag=dag
)

# 8. Ingestar datos chunked en Druid - TODOS LOS ANÁLISIS
druid_symptoms_task = BashOperator(
    task_id='ingest_chunked_symptoms_to_druid',
    bash_command="""
    echo "⏳ Verificando archivos para ingesta de síntomas..."
    
    # Verificar que los archivos existan
    SPEC_FILE="/opt/shared_data/druid_ingestion_specs/symptoms_ingestion.json"
    DATA_FILE="/opt/shared_data/vaers_results/chunked_symptoms_for_druid.json"
    
    if [ ! -f "$SPEC_FILE" ]; then
        echo "❌ Archivo de especificación no encontrado: $SPEC_FILE"
        exit 1
    fi
    
    if [ ! -f "$DATA_FILE" ]; then
        echo "❌ Archivo de datos no encontrado: $DATA_FILE"
        exit 1
    fi
    
    echo "✅ Archivos verificados, enviando tarea a Druid..."
    
    # Enviar tarea a Druid con manejo de errores
    RESPONSE=$(curl -s -w "%{http_code}" -X POST "http://router:8888/druid/indexer/v1/task" \
        -H "Content-Type: application/json" \
        -d @"$SPEC_FILE")
    
    HTTP_CODE="${RESPONSE: -3}"
    BODY="${RESPONSE%???}"
    
    echo "HTTP Code: $HTTP_CODE"
    echo "Response: $BODY"
    
    if [ "$HTTP_CODE" -ne 200 ]; then
        echo "❌ Error en ingesta de síntomas - HTTP $HTTP_CODE"
        exit 1
    fi
    
    echo "✅ Tarea de síntomas enviada exitosamente"
    """,
    dag=dag
)

# 9. Ingestar análisis de severidad
druid_severity_task = BashOperator(
    task_id='ingest_severity_to_druid',
    bash_command="""
    echo "⏳ Verificando archivos para ingesta de severidad..."
    sleep 3
    
    # Verificar que los archivos existan
    SPEC_FILE="/opt/shared_data/druid_ingestion_specs/severity_ingestion.json"
    DATA_FILE="/opt/shared_data/vaers_results/severity_for_druid.json"
    
    if [ ! -f "$SPEC_FILE" ]; then
        echo "❌ Archivo de especificación no encontrado: $SPEC_FILE"
        exit 1
    fi
    
    if [ ! -f "$DATA_FILE" ]; then
        echo "❌ Archivo de datos no encontrado: $DATA_FILE"
        exit 1
    fi
    
    echo "✅ Archivos verificados, enviando tarea a Druid..."
    
    # Enviar tarea a Druid con manejo de errores
    RESPONSE=$(curl -s -w "%{http_code}" -X POST "http://router:8888/druid/indexer/v1/task" \
        -H "Content-Type: application/json" \
        -d @"$SPEC_FILE")
    
    HTTP_CODE="${RESPONSE: -3}"
    BODY="${RESPONSE%???}"
    
    echo "HTTP Code: $HTTP_CODE"
    echo "Response: $BODY"
    
    if [ "$HTTP_CODE" -ne 200 ]; then
        echo "❌ Error en ingesta de severidad - HTTP $HTTP_CODE"
        exit 1
    fi
    
    echo "✅ Tarea de severidad enviada exitosamente"
    """,
    dag=dag
)

# 10. Ingestar análisis geográfico
druid_geographic_task = BashOperator(
    task_id='ingest_geographic_to_druid',
    bash_command="""
    echo "⏳ Verificando archivos para ingesta geográfica..."
    sleep 3
    
    # Verificar que los archivos existan
    SPEC_FILE="/opt/shared_data/druid_ingestion_specs/geographic_ingestion.json"
    DATA_FILE="/opt/shared_data/vaers_results/geographic_for_druid.json"
    
    if [ ! -f "$SPEC_FILE" ]; then
        echo "❌ Archivo de especificación no encontrado: $SPEC_FILE"
        exit 1
    fi
    
    if [ ! -f "$DATA_FILE" ]; then
        echo "❌ Archivo de datos no encontrado: $DATA_FILE"
        exit 1
    fi
    
    echo "✅ Archivos verificados, enviando tarea a Druid..."
    
    # Enviar tarea a Druid con manejo de errores
    RESPONSE=$(curl -s -w "%{http_code}" -X POST "http://router:8888/druid/indexer/v1/task" \
        -H "Content-Type: application/json" \
        -d @"$SPEC_FILE")
    
    HTTP_CODE="${RESPONSE: -3}"
    BODY="${RESPONSE%???}"
    
    echo "HTTP Code: $HTTP_CODE"
    echo "Response: $BODY"
    
    if [ "$HTTP_CODE" -ne 200 ]; then
        echo "❌ Error en ingesta geográfica - HTTP $HTTP_CODE"
        exit 1
    fi
    
    echo "✅ Tarea geográfica enviada exitosamente"
    """,
    dag=dag
)

# 11. Refrescar datasets en Superset
refresh_datasets_task = PythonOperator(
    task_id='refresh_superset_datasets',
    python_callable=refresh_superset_datasets,
    dag=dag
)

# 12. Configurar Superset dashboards
superset_setup_task = PythonOperator(
    task_id='setup_superset_dashboards',
    python_callable=setup_superset_dashboards,
    dag=dag
)

# 13. Tarea final de verificación
verification_task = BashOperator(
    task_id='final_verification',
    bash_command="""
    echo "🎉 Pipeline VAERS con Polars completado!"
    echo "📊 Datos disponibles en:"
    echo "   - Druid: http://localhost:8888"
    echo "   - Superset: http://localhost:8088"
    echo "   - PostgreSQL: puerto 5432"
    echo ""
    echo "📁 Archivos generados en /opt/shared_data/vaers_results:"
    ls -la /opt/shared_data/vaers_results/ || echo "Directorio no accesible"
    """,
    dag=dag
)

# ===== DEPENDENCIAS =====

# Flujo principal
check_data_task >> setup_directories_task >> quality_check_task
quality_check_task >> polars_etl_task
polars_etl_task >> [druid_connectivity_task, druid_prep_task, postgres_load_task]

# IMPORTANTE: Verificar Druid antes de ingestar datos
druid_connectivity_task >> [druid_symptoms_task, druid_severity_task, druid_geographic_task]

# IMPORTANTE: Druid después de que ETL Y preparación estén completos
[druid_prep_task, polars_etl_task] >> druid_symptoms_task
[druid_prep_task, polars_etl_task] >> druid_severity_task
[druid_prep_task, polars_etl_task] >> druid_geographic_task

# Refresh datasets después de Druid
[druid_symptoms_task, druid_severity_task, druid_geographic_task] >> refresh_datasets_task

# Superset después de PostgreSQL Y refresh de datasets
[postgres_load_task, refresh_datasets_task] >> superset_setup_task

# Verificación final después de todo
superset_setup_task >> verification_task
