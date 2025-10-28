#!/usr/bin/env python3
"""
Este archivo contiene funciones de utilidad para procesamiento
de datos, conectividad y configuración
"""

import json
import logging
import os
import subprocess
import sys
import time
import pandas as pd
import polars as pl
import requests
from sqlalchemy import create_engine
sys.path.append('/opt/airflow/etl')
from etl_processor import run_chunked_pipeline # type: ignore

# ============================================================ CONSTANTES ============================================================

VAERS_DATA_DIR = "/opt/airflow/data"
VAERS_RESULTS_DIR = "/opt/shared_data/vaers_results"
REQUIRED_FILES = ["VAERSDATA.csv", "VAERSSYMPTOMS.csv", "VAERSVAX.csv"]


# ======================================= FUNCIONES DE CONFIGURACIÓN Y VERIFICACIÓN ==================================================

# Función para preparar el directorio compartido
def setup_shared_directories():
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

    return "🎉 Directorio compartido verificado correctamente"

# Función para verificar la calidad de los datos de entrada
def check_data_quality():
    for file in REQUIRED_FILES:
        file_path = f"{VAERS_DATA_DIR}/{file}"
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"❌ Archivo requerido no encontrado: {file}")

        # Verificar que el archivo no esté vacío
        try:
            df = pl.read_csv(file_path, n_rows=5)
            if df.height == 0:
                raise ValueError(f"❌ Archivo vacío: {file}")
            print(f"✅ {file} verificado - {df.height} filas y {df.width} columnas")
        except Exception as e:
            print(f"❌ Error verificando {file}: {e}")
            raise

    return "🎉 Datos verificados correctamente"

# Función para verificar que los requeridos archivos existen
def check_required_files():
    print(f"🔍 Verificando archivos en {VAERS_DATA_DIR}...")
    
    for file in REQUIRED_FILES:
        file_path = f"{VAERS_DATA_DIR}/{file}"
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"❌ Archivo requerido no encontrado: {file_path}")

        # Verificar tamaño del archivo
        size = os.path.getsize(file_path)
        size_mb = size / 1024 / 1024
        print(f"✅ {file}: {size_mb:.1f} MB")
    
    print("✅ Todos los archivos requeridos están disponibles")
    
    return "🎉 Archivos verificados correctamente"

# ================================================== FUNCIONES DE PROCESAMIENTO ETL ==================================================

# Función para ejecutar ETL
def run_polars_etl():
    logger = logging.getLogger(__name__)

    try:
        logger.info("🚀 Iniciando ETL VAERS...")
        result = run_chunked_pipeline()
        logger.info("✅ ETL completado: %s", result)
        return result
    except Exception as e:
        logger.error("❌ Error en ETL: %s", str(e))
        raise

# ======================================================== FUNCIONES DE DRUID ========================================================

# Función para generar especificaciones de Druid
def make_druid_spec(filename, data_source, dimensions, metrics):
    return {
        "type": "index_parallel",
        "spec": {
            "ioConfig": {
                "type": "index_parallel",
                "inputSource": {
                    "type": "local",
                    "baseDir": VAERS_RESULTS_DIR,
                    "filter": filename
                },
                "inputFormat": {"type": "json"}
            },
            "tuningConfig": {"type": "index_parallel", "partitionsSpec": {"type": "dynamic"}},
            "dataSchema": {
                "dataSource": data_source,
                "timestampSpec": {"column": "__time", "format": "iso"},
                "dimensionsSpec": {"dimensions": dimensions},
                "metricsSpec": metrics,
                "granularitySpec": {"type": "uniform", "segmentGranularity": "DAY", "queryGranularity": "NONE"}
            }
        }
    }

# Función para preparar datos para Druid
def prepare_druid_ingestion():
    spec_path = "/opt/shared_data/druid_ingestion_specs"

    # Verificar que los directorios existen
    if not os.path.exists(spec_path):
        raise Exception(f"❌ Directorio de specs no existe: {spec_path}")
    if not os.path.exists(VAERS_RESULTS_DIR):
        raise Exception(f"❌ Directorio de datos no existe: {VAERS_RESULTS_DIR}")

    print(f"Usando volumen compartido para Druid: {spec_path}")

    symptoms_spec = make_druid_spec(
        "chunked_symptoms_for_druid.json",
        "vaers_symptoms_by_manufacturer",
        ["manufacturer", "symptom"],
        [
            {"name": "total_reports", "type": "longSum", "fieldName": "total_reports"},
            {"name": "deaths", "type": "longSum", "fieldName": "deaths"},
            {"name": "hospitalizations", "type": "longSum", "fieldName": "hospitalizations"}
        ]
    )

    severity_spec = make_druid_spec(
        "severity_for_druid.json",
        "vaers_severity_by_age",
        ["age_group", "VAX_MANU_CLEAN"],
        [
            {"name": "total_reports", "type": "longSum", "fieldName": "total_cases"},
            {"name": "deaths", "type": "longSum", "fieldName": "deaths"},
            {"name": "hospitalizations", "type": "longSum", "fieldName": "hospitalizations"},
            {"name": "er_visits", "type": "longSum", "fieldName": "er_visits"},
            {"name": "severe_cases", "type": "longSum", "fieldName": "severe_cases"},
            {"name": "death_rate", "type": "doubleSum", "fieldName": "death_rate"},
            {"name": "hospital_rate", "type": "doubleSum", "fieldName": "hospital_rate"},
            {"name": "severe_rate", "type": "doubleSum", "fieldName": "severe_rate"},
            {"name": "avg_age", "type": "doubleSum", "fieldName": "avg_age"}
        ]
    )

    geographic_spec = make_druid_spec(
        "geographic_for_druid.json",
        "vaers_geographic_distribution",
        ["state", "VAX_MANU_CLEAN"],
        [
            {"name": "total_reports", "type": "longSum", "fieldName": "total_reports"},
            {"name": "deaths", "type": "longSum", "fieldName": "deaths"},
            {"name": "hospitalizations", "type": "longSum", "fieldName": "hospitalizations"},
            {"name": "er_visits", "type": "longSum", "fieldName": "er_visits"},
            {"name": "avg_age", "type": "doubleSum", "fieldName": "avg_age"},
            {"name": "death_rate", "type": "doubleSum", "fieldName": "death_rate"},
            {"name": "hospital_rate", "type": "doubleSum", "fieldName": "hospital_rate"}
        ]
    )

    # Guardar todas las especificaciones
    specs = {
        "symptoms_ingestion.json": symptoms_spec,
        "severity_ingestion.json": severity_spec,
        "geographic_ingestion.json": geographic_spec
    }

    for filename, spec in specs.items():
        spec_file = os.path.join(spec_path, filename)
        with open(spec_file, 'w') as f:
            json.dump(spec, f, indent=2)
        print(f"✔️ Especificación creada: {spec_file}")

    return f"🎉 Especificaciones de Druid creadas en {spec_path}"

# Función para verificar conectividad con Druid
def check_druid_connectivity():
    druid_url = "http://router:8888/status"
    max_retries = 10
    retry_delay = 30
    
    print("🔍 Verificando conectividad con Druid...")
    
    for attempt in range(max_retries):
        try:
            response = requests.get(druid_url, timeout=10)
            if response.status_code == 200:
                print(f"✅ Druid disponible en {druid_url}")
                return "🎉 Druid conectado exitosamente"
        except Exception as e:
            print(f"⚠️ Intento {attempt + 1}/{max_retries} falló: {e}")
        
        if attempt < max_retries - 1:
            print(f"⏳ Esperando {retry_delay}s antes del siguiente intento...")
            time.sleep(retry_delay)
    
    raise Exception("❌ No se pudo conectar con Druid después de múltiples intentos")

# ===================================================== FUNCIONES DE POSTGRESQL ======================================================

# Función para cargar datos a PostgreSQL
def load_to_postgresql():
    # Conexión a PostgreSQL
    engine = create_engine('postgresql://superset:superset@postgres:5432/superset')

    # Archivos chunked a cargar
    files_to_load = [
        ("chunked_symptoms_analysis.csv", "vaers_symptoms_analysis"),
        ("severity_analysis.csv", "vaers_severity_analysis"),
        ("geographic_analysis.csv", "vaers_geographic_analysis")
    ]

    for csv_file, table_name in files_to_load:
        file_path = f"{VAERS_RESULTS_DIR}/{csv_file}"

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

    return "🎉 Datos cargados a PostgreSQL correctamente"

# ======================================================= FUNCIONES DE SUPERSET ======================================================

# Función para refrescar datasets en Superset
# Sirve para sincronizar columnas con Druid
def refresh_superset_datasets():
    print("🔄 Refrescando datasets en Superset...")
    time.sleep(10)

    try:
        # Ejecutar script de refresh
        result = subprocess.run([
            'python', '/opt/airflow/superset/dataset_manager.py'
        ], capture_output=True, text=True, timeout=120)

        if result.returncode == 0:
            print("✅ Datasets refrescados exitosamente")
            print(result.stdout)
            return "🎉 Datasets refrescados correctamente"
        else:
            print(f"❌ Error refrescando datasets: {result.stderr}")
            return "⚠️ Error refrescando datasets"
        
    except Exception as e:
        print(f"❌ Error ejecutando refresh: {e}")
        return "⚠️ Error en refresh de datasets"

# Función para configurar los dashboards en Superset
def setup_superset_dashboards():
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
            return "🎉 Dashboards configurados correctamente"
        else:
            print(f"❌ Error configurando dashboards: {result.stderr}")
            raise Exception(f"Error en configuración de Superset: {result.stderr}")

    except subprocess.TimeoutExpired:
        print("⏰ Timeout configurando Superset")
        raise
    except Exception as e:
        print(f"❌ Error ejecutando configuración de Superset: {e}")
        raise