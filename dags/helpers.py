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

# Función para generar comando de ingesta de Druid
def generate_druid_ingestion_command(ingestion_type, spec_filename, data_filename):
    """Generar comando bash parametrizado para ingesta de Druid"""
    return f"""
    echo "⏳ Verificando archivos para ingesta de {ingestion_type}..."
    
    # Configuración de archivos
    SPEC_FILE="/opt/shared_data/druid_ingestion_specs/{spec_filename}"
    DATA_FILE="/opt/shared_data/vaers_results/{data_filename}"
    
    # Verificar que los archivos existan
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
    RESPONSE=$(curl -s -w "%{{http_code}}" -X POST "http://router:8888/druid/indexer/v1/task" \\
        -H "Content-Type: application/json" \\
        -d @"$SPEC_FILE")
    
    HTTP_CODE="${{RESPONSE: -3}}"
    BODY="${{RESPONSE%???}}"
    
    echo "📊 HTTP Code: $HTTP_CODE"
    echo "📋 Response: $BODY"
    
    # Validar respuesta
    if [ "$HTTP_CODE" -ne 200 ]; then
        echo "❌ Error en ingesta de {ingestion_type} - HTTP $HTTP_CODE"
        echo "💡 Revisar conectividad con Druid y formato de archivos"
        exit 1
    fi
    
    echo "✅ Tarea de {ingestion_type} enviada exitosamente"
    """

# Función para generar especificaciones de Druid
def make_druid_spec(filename, data_source, dimensions, metrics):
    """Generar especificación de ingesta para Druid con reemplazo de datos"""
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
                "inputFormat": {"type": "json"},
                # Configurar para reemplazar datos existentes
                "appendToExisting": False
            },
            "tuningConfig": {
                "type": "index_parallel", 
                "partitionsSpec": {"type": "hashed", "numShards": 1},
                # Forzar reemplazo de segmentos - compatible con hashed partitions
                "forceGuaranteedRollup": True
            },
            "dataSchema": {
                "dataSource": data_source,
                "timestampSpec": {
                    "column": "__time", 
                    "format": "iso",
                    # Usar timestamp actual si falta
                    "missingValue": "1970-01-01T00:00:00Z"
                },
                "dimensionsSpec": {"dimensions": dimensions},
                "metricsSpec": metrics,
                "granularitySpec": {
                    "type": "uniform", 
                    "segmentGranularity": "DAY", 
                    "queryGranularity": "NONE",
                    # Configurar intervalo para datos actuales
                    "rollup": False
                }
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

def cleanup_druid_datasource(datasource_name):
    """Limpiar datasource existente en Druid para evitar datos obsoletos"""
    
    print(f"🧹 Limpiando datasource existente: {datasource_name}")
    
    try:
        # Obtener información del datasource
        datasource_url = f"http://router:8888/druid/coordinator/v1/datasources/{datasource_name}"
        response = requests.get(datasource_url, timeout=10)
        
        if response.status_code == 200:
            print(f"📋 Datasource {datasource_name} existe, marcando para limpieza...")
            
            # Deshabilitar datasource
            disable_url = f"http://router:8888/druid/coordinator/v1/datasources/{datasource_name}"
            disable_response = requests.delete(disable_url, timeout=30)
            
            if disable_response.status_code in [200, 202]:
                print(f"✅ Datasource {datasource_name} deshabilitado exitosamente")
            else:
                print(f"⚠️ No se pudo deshabilitar {datasource_name}: HTTP {disable_response.status_code}")
        else:
            print(f"ℹ️ Datasource {datasource_name} no existe, continuando...")
            
    except Exception as e:
        print(f"⚠️ Error limpiando datasource {datasource_name}: {e}")
        # No fallar el pipeline por esto
    
    # Esperar un momento para que Druid procese la limpieza
    time.sleep(5)

def cleanup_all_druid_datasources():
    """Limpiar todos los datasources de VAERS en Druid antes de nueva ingesta"""
    
    datasources_to_cleanup = [
        "vaers_symptoms_by_manufacturer",
        "vaers_severity_by_age", 
        "vaers_geographic_distribution"
    ]
    
    print("🗑️ Iniciando limpieza de datasources de Druid...")
    print(f"📅 Fecha de ejecución: {time.strftime('%Y-%m-%d %H:%M:%S')}")
    
    for datasource in datasources_to_cleanup:
        cleanup_druid_datasource(datasource)
    
    print("✅ Limpieza de datasources completada")
    return "Datasources de Druid limpiados exitosamente"

def create_druid_ingestion_task(task_id, ingestion_type, spec_filename, data_filename, dag, datasource_name=None, sleep_seconds=0):
    """Factory function para crear tareas de ingesta de Druid parametrizadas con limpieza"""
    from airflow.operators.bash_operator import BashOperator
    
    # Generar comando con sleep opcional
    sleep_cmd = f"sleep {sleep_seconds}\n    " if sleep_seconds > 0 else ""
    
    # Comando de limpieza opcional si se proporciona datasource_name
    cleanup_cmd = ""
    if datasource_name:
        cleanup_cmd = f"""
    echo "🧹 Limpiando datasource existente: {datasource_name}..."
    curl -s -X DELETE "http://router:8888/druid/coordinator/v1/datasources/{datasource_name}" || echo "⚠️ Datasource no existía"
    sleep 3
    """
    
    bash_command = f"""
    {sleep_cmd}echo "⏳ Verificando archivos para ingesta de {ingestion_type}..."
    
    # Configuración de archivos
    SPEC_FILE="/opt/shared_data/druid_ingestion_specs/{spec_filename}"
    DATA_FILE="/opt/shared_data/vaers_results/{data_filename}"
    
    # Verificar que los archivos existan
    if [ ! -f "$SPEC_FILE" ]; then
        echo "❌ Archivo de especificación no encontrado: $SPEC_FILE"
        exit 1
    fi
    
    if [ ! -f "$DATA_FILE" ]; then
        echo "❌ Archivo de datos no encontrado: $DATA_FILE"
        exit 1
    fi
    
    echo "✅ Archivos verificados para {ingestion_type}"
    echo "📅 Timestamp en datos: $(head -1 "$DATA_FILE" | grep -o '"__time":"[^"]*"' || echo 'No encontrado')"
    
    {cleanup_cmd}
    
    echo "🚀 Enviando tarea a Druid..."
    
    # Enviar tarea a Druid con manejo de errores mejorado
    RESPONSE=$(curl -s -w "%{{http_code}}" -X POST "http://router:8888/druid/indexer/v1/task" \\
        -H "Content-Type: application/json" \\
        -d @"$SPEC_FILE")
    
    HTTP_CODE="${{RESPONSE: -3}}"
    BODY="${{RESPONSE%???}}"
    
    echo "📊 HTTP Code: $HTTP_CODE"
    echo "📋 Response Body: $BODY"
    
    # Validar respuesta
    if [ "$HTTP_CODE" -eq 200 ] || [ "$HTTP_CODE" -eq 202 ]; then
        echo "✅ Tarea de {ingestion_type} enviada exitosamente"
        echo "🆔 Task ID: $(echo "$BODY" | grep -o '"task":"[^"]*"' | cut -d'"' -f4)"
    else
        echo "❌ Error en ingesta de {ingestion_type} - HTTP $HTTP_CODE"
        echo "💡 Revisar conectividad con Druid y formato de archivos"
        echo "🔍 Response completo: $BODY"
        exit 1
    fi
    """
    
    return BashOperator(
        task_id=task_id,
        bash_command=bash_command,
        dag=dag
    )

# ===== FUNCIONES DE POSTGRESQL =====

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

# Función UNIFICADA para configurar TODO Superset en un solo paso
def setup_superset_complete():
    """
    Configura COMPLETAMENTE Superset en un solo paso:
    - Conexión DB PostgreSQL
    - Datasets VAERS (crea o refresca)
    - Dashboard con gráficos
    """
    print("🚀 Configuración completa de Superset...")
    time.sleep(20)  # Esperar a que Superset esté listo

    try:
        # Ejecutar script unificado de configuración completa
        result = subprocess.run([
            'python', '/opt/airflow/superset/complete_setup.py'
        ], capture_output=True, text=True, timeout=300)

        # Siempre imprimir stdout
        if result.stdout:
            print("📋 Salida:")
            print(result.stdout)
        
        if result.stderr:
            print("⚠️ Errores/Warnings:")
            print(result.stderr)

        if result.returncode == 0:
            print("✅ Superset configurado exitosamente!")
            return "🎉 Superset configurado correctamente"
        else:
            print(f"❌ Script terminó con código de error: {result.returncode}")
            raise Exception(f"Error configurando Superset. Código: {result.returncode}")

    except subprocess.TimeoutExpired:
        print("⏰ Timeout configurando Superset (>5 minutos)")
        raise
    except Exception as e:
        print(f"❌ Error ejecutando configuración de Superset: {e}")
        import traceback
        traceback.print_exc()
        raise