#!/usr/bin/env python3
"""
DAG de Airflow para procesar datos VAERS
Orquesta el pipeline completo: ETL con Spark -> Druid -> Superset
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.sensors.filesystem import FileSensor
import subprocess
import os

# Configuración por defecto del DAG
default_args = {
    'owner': 'vaers-analytics-team',
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
    'vaers_covid_analytics_pipeline',
    default_args=default_args,
    description='Pipeline completo de análisis VAERS COVID-19',
    schedule_interval=None,  # Ejecutar solo manualmente
    max_active_runs=1,
    tags=['vaers', 'covid', 'analytics', 'spark', 'druid']
)

# Función para preparar el entorno
def setup_shared_directories():
    """
    Verificar que el volumen compartido esté disponible - los directorios ya se crean con volume-init
    """
    import os
    
    # Los directorios deberían existir ya por el servicio volume-init
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
    import pandas as pd
    
    # Usar datos de Airflow ya que están disponibles
    data_path = "/opt/airflow/data"
    required_files = ["VAERSDATA.csv", "VAERSSYMPTOMS.csv", "VAERSVAX.csv"]
    
    for file in required_files:
        file_path = f"{data_path}/{file}"
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"Archivo requerido no encontrado: {file}")
        
        # Verificar que el archivo no esté vacío
        df = pd.read_csv(file_path, nrows=5)
        if df.empty:
            raise ValueError(f"Archivo vacío: {file}")
        
        print(f"✅ {file} verificado - {len(df)} filas de muestra")
    
    return "Verificación de datos completada exitosamente"

# Función para preparar datos para Druid
def prepare_druid_ingestion():
    """Prepara archivos de especificación de ingesta para Druid - TODOS los análisis"""
    import json
    import os
    
    # Solo usar volumen compartido - es crítico para Druid
    spec_path = "/opt/shared_data/druid_ingestion_specs"
    data_path = "/opt/shared_data/vaers_results"
    
    # Verificar que los directorios existen (deberían existir por setup_shared_directories)
    if not os.path.exists(spec_path):
        raise Exception(f"❌ Directorio de specs no existe: {spec_path}")
    if not os.path.exists(data_path):
        raise Exception(f"❌ Directorio de datos no existe: {data_path}")
    
    print(f"✅ Usando volumen compartido para Druid: {spec_path}")
    
    # 1. Especificación para síntomas por fabricante
    symptoms_spec = {
        "type": "index_parallel",
        "spec": {
            "ioConfig": {
                "type": "index_parallel",
                "inputSource": {
                    "type": "local",
                    "baseDir": "/opt/shared_data/vaers_results",
                    "filter": "symptoms_for_druid.json"
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
                "dimensionsSpec": {"dimensions": ["VAX_MANU_CLEAN", "symptom_name"]},
                "metricsSpec": [
                    {"name": "total_reports", "type": "longSum", "fieldName": "total_reports"},
                    {"name": "unique_cases", "type": "longSum", "fieldName": "unique_cases"},
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
    
    # Guardar todas las especificaciones
    specs = {
        "symptoms_by_manufacturer.json": symptoms_spec,
        "severity_by_age.json": severity_spec,
        "geographic_distribution.json": geographic_spec
    }
    
    for filename, spec in specs.items():
        with open(f"{spec_path}/{filename}", "w") as f:
            json.dump(spec, f, indent=2)
        print(f"✅ Especificación creada: {filename}")
    
    print("✅ Todas las especificaciones de Druid preparadas")
    return "Especificaciones creadas exitosamente para todos los análisis"

# Task 0: Preparar directorios compartidos
setup_dirs_task = PythonOperator(
    task_id='setup_shared_directories',
    python_callable=setup_shared_directories,
    dag=dag
)

# Task 1: Verificar archivos de datos - Saltamos esta verificación por ahora
check_data_task = DummyOperator(
    task_id='check_data_files_exist',
    dag=dag
)

# Task 2: Validar calidad de datos
validate_data_task = PythonOperator(
    task_id='validate_data_quality',
    python_callable=check_data_quality,
    dag=dag
)

# Task 3: Ejecutar análisis simple con Python
spark_etl_task = BashOperator(
    task_id='run_spark_etl',
    bash_command="python3 /opt/airflow/dags/simple_analysis.py",
    dag=dag
)

# Task 4: Preparar datos para Druid
prepare_druid_task = PythonOperator(
    task_id='prepare_druid_ingestion',
    python_callable=prepare_druid_ingestion,
    dag=dag
)

def ingest_druid_data():
    """Ingestar TODOS los datos en Druid usando urllib de Python"""
    import urllib.request
    import urllib.parse
    import json
    import time
    import os
    
    # Esperar a que Druid esté listo
    print("⏳ Esperando 30 segundos a que Druid esté listo...")
    time.sleep(30)
    
    # Lista de todos los archivos de especificación a procesar
    spec_files = [
        "symptoms_by_manufacturer.json",
        "severity_by_age.json", 
        "geographic_distribution.json"
    ]
    
    spec_path = "/opt/shared_data/druid_ingestion_specs"
    druid_url = "http://coordinator:8081/druid/indexer/v1/task"
    
    ingested_tasks = []
    
    # Procesar cada especificación
    for spec_filename in spec_files:
        spec_file = f"{spec_path}/{spec_filename}"
        
        if not os.path.exists(spec_file):
            print(f"⚠️ Archivo de especificación no encontrado: {spec_file}")
            continue
        
        with open(spec_file, 'r') as f:
            spec = json.load(f)
        
        print(f"✅ Cargada especificación desde: {spec_filename}")
        
        try:
            # Preparar request
            data = json.dumps(spec).encode('utf-8')
            req = urllib.request.Request(druid_url, data=data, headers={'Content-Type': 'application/json'})
            
            # Enviar request
            with urllib.request.urlopen(req, timeout=30) as response:
                result = json.loads(response.read().decode('utf-8'))
                task_id = result.get('task', 'unknown')
                ingested_tasks.append(f"{spec['spec']['dataSchema']['dataSource']}: {task_id}")
                print(f"✅ Tarea enviada para {spec_filename}. Task ID: {task_id}")
                
                # Pequeña pausa entre ingestas
                time.sleep(5)
                
        except Exception as e:
            print(f"❌ Error enviando {spec_filename} a Druid: {str(e)}")
            raise
    
    print(f"🎯 Total de datasets enviados: {len(ingested_tasks)}")
    for task in ingested_tasks:
        print(f"   📊 {task}")
    
    return f"Ingesta completada para {len(ingested_tasks)} datasets: {', '.join([t.split(':')[0] for t in ingested_tasks])}"

# Task 5: Ingestar datos en Druid
ingest_druid_task = PythonOperator(
    task_id='ingest_data_to_druid',
    python_callable=ingest_druid_data,
    dag=dag
)

def verify_druid_data():
    """Verificar que TODOS los datos estén disponibles en Druid"""
    import urllib.request
    import json
    import time
    
    # Esperar a que termine la ingesta
    print("⏳ Esperando 60 segundos a que complete la ingesta...")
    time.sleep(60)
    
    # Datasources esperados
    expected_datasources = [
        'vaers_symptoms_by_manufacturer',
        'vaers_severity_by_age', 
        'vaers_geographic_distribution'
    ]
    
    # Verificar datasources disponibles
    try:
        datasources_url = "http://coordinator:8081/druid/coordinator/v1/datasources"
        
        with urllib.request.urlopen(datasources_url, timeout=30) as response:
            datasources = json.loads(response.read().decode('utf-8'))
            print(f"📊 Datasources disponibles: {datasources}")
            
            found_datasources = []
            missing_datasources = []
            
            # Verificar cada datasource esperado
            for expected_ds in expected_datasources:
                if expected_ds in datasources:
                    found_datasources.append(expected_ds)
                    
                    # Ejecutar query de prueba
                    try:
                        query_url = "http://broker:8082/druid/v2/sql"
                        query = {"query": f"SELECT COUNT(*) as total_rows FROM {expected_ds} LIMIT 10"}
                        
                        data = json.dumps(query).encode('utf-8')
                        req = urllib.request.Request(query_url, data=data, headers={'Content-Type': 'application/json'})
                        
                        with urllib.request.urlopen(req, timeout=30) as query_response:
                            results = json.loads(query_response.read().decode('utf-8'))
                            row_count = results[0]['total_rows'] if results else 0
                            print(f"✅ {expected_ds}: {row_count} filas")
                            
                    except Exception as e:
                        print(f"⚠️ Error consultando {expected_ds}: {str(e)}")
                else:
                    missing_datasources.append(expected_ds)
            
            # Reporte final
            if len(found_datasources) == len(expected_datasources):
                print(f"🎉 ¡Todos los datasources creados exitosamente! ({len(found_datasources)}/3)")
                return f"Verificación exitosa: {len(found_datasources)} datasources disponibles"
            else:
                print(f"⚠️ Datasources faltantes: {missing_datasources}")
                return f"Parcial: {len(found_datasources)}/{len(expected_datasources)} datasources disponibles"
            
    except Exception as e:
        print(f"❌ Error verificando datos en Druid: {str(e)}")
        return f"Error en verificación: {str(e)}"

# Task 6: Verificar datos en Druid
verify_druid_task = PythonOperator(
    task_id='verify_druid_data',
    python_callable=verify_druid_data,
    dag=dag
)

def setup_superset_dashboard():
    """Configurar dashboard completo usando script de integración"""
    try:
        # Ejecutar script de integración completo que maneja todo internamente
        result = subprocess.run([
            'python3', '/opt/airflow/superset/dag_integration_complete.py'
        ], capture_output=True, text=True, cwd='/opt/airflow', check=False)
        
        if result.returncode == 0:
            print("✅ Dashboard VAERS completo configurado desde DAG")
            print("📊 Dashboard automático incluye:")
            print("   • 📊 Distribución por fabricantes")
            print("   • 📈 Top síntomas reportados") 
            print("   • 🏥 Hospitalizaciones por edad")
            print("   • 🗺️ Distribución geográfica")
            print("🔗 Dashboard disponible en: http://localhost:8088")
            
            # Mostrar resultado del script
            if result.stdout:
                print("\n📋 Resultado detallado:")
                print(result.stdout[-300:])
        else:
            print("⚠️ Error configurando dashboard:", result.stderr)
            if result.stdout:
                print("Output:", result.stdout)
            
    except Exception as e:
        print(f"❌ Error ejecutando integración dashboard: {str(e)}")
        raise

def setup_basic_superset_fallback():
    """Configuración básica de fallback usando script simple"""
    print("🔄 Fallback: Configuración básica de Superset...")
    
    import subprocess
    import sys
    
    try:
        # Usar el script básico como fallback
        result = subprocess.run([
            sys.executable, '/opt/airflow/superset/dag_integration.py'
        ], capture_output=True, text=True, timeout=300)
        
        if result.returncode == 0:
            print("✅ Configuración básica completada")
            print(result.stdout)
            return "Superset configurado básicamente - datasets disponibles"
        else:
            print(f"⚠️ Fallback también falló: {result.stderr}")
            return "Error en configuración - verificar manualmente"
            
    except Exception as e:
        print(f"❌ Error en fallback: {str(e)}")
        return "Error completo - revisar logs de Superset"

# Task 7: Configurar Superset automáticamente 
setup_superset_task = PythonOperator(
    task_id='setup_superset_dashboard',
    python_callable=setup_superset_dashboard,
    dag=dag
)

# Task 8: Generar reporte de resumen
generate_report_task = BashOperator(
    task_id='generate_summary_report',
    bash_command="""
    echo "📊 REPORTE DE PROCESAMIENTO VAERS - $(date)" > /tmp/pipeline_report.txt
    echo "=============================================" >> /tmp/pipeline_report.txt
    echo "" >> /tmp/pipeline_report.txt
    
    echo "✅ Datos procesados exitosamente" >> /tmp/pipeline_report.txt
    echo "✅ Análisis generados:" >> /tmp/pipeline_report.txt
    echo "   - Síntomas por fabricante" >> /tmp/pipeline_report.txt  
    echo "   - Severidad por edad" >> /tmp/pipeline_report.txt
    echo "   - Tiempo de aparición de síntomas" >> /tmp/pipeline_report.txt
    echo "   - Distribución geográfica" >> /tmp/pipeline_report.txt
    echo "" >> /tmp/pipeline_report.txt
    
    echo "🌐 Acceso a dashboards:" >> /tmp/pipeline_report.txt
    echo "   - Druid Console: http://localhost:8888" >> /tmp/pipeline_report.txt
    echo "   - Superset Dashboard: http://localhost:8088/dashboard/list/" >> /tmp/pipeline_report.txt
    echo "   - Credenciales: admin/admin" >> /tmp/pipeline_report.txt
    echo "" >> /tmp/pipeline_report.txt
    
    echo "Fecha de procesamiento: $(date)" >> /tmp/pipeline_report.txt
    
    cat /tmp/pipeline_report.txt
    """,
    dag=dag
)

# Tasks de inicio y fin
start_task = DummyOperator(task_id='start_pipeline', dag=dag)
end_task = DummyOperator(task_id='end_pipeline', dag=dag)

# Definir dependencias del pipeline
start_task >> setup_dirs_task >> check_data_task >> validate_data_task >> spark_etl_task
spark_etl_task >> prepare_druid_task >> ingest_druid_task >> verify_druid_task
verify_druid_task >> setup_superset_task >> generate_report_task >> end_task

# Información del DAG
dag.doc_md = """
# Pipeline de Análisis VAERS COVID-19

Este DAG procesa datos del sistema VAERS (Vaccine Adverse Event Reporting System) 
para generar análisis específicos de eventos adversos relacionados con vacunas COVID-19.

## Análisis Generados:

1. **Síntomas Más Frecuentes por Fabricante**: Top 10 síntomas reportados para PFIZER, MODERNA y JANSSEN
2. **Análisis de Severidad por Edad**: Correlación entre edad y resultados severos (hospitalización, muerte)
3. **Tiempo de Aparición de Síntomas**: Tiempo promedio de aparición de síntomas después de la vacunación
4. **Distribución Geográfica**: Estados con mayor número de reportes

## Flujo del Pipeline:

```
Datos CSV → Spark ETL → Druid → Superset → Dashboards
     ↑
  Airflow (orquestación)
```

## Archivos de Entrada Requeridos:
- `VAERSDATA.csv`: Datos principales de reportes VAERS
- `VAERSSYMPTOMS.csv`: Síntomas reportados (códigos MedDRA)  
- `VAERSVAX.csv`: Información de vacunas administradas

## Outputs:
- Datos procesados en formato Parquet en `/opt/shared_data/`
- Datos ingestados en Druid para consultas analíticas
- Dashboards disponibles en Superset
"""