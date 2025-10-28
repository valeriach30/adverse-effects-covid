from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator # type: ignore
from airflow.operators.python_operator import PythonOperator # type: ignore

from helpers import (
    setup_shared_directories,
    check_data_quality,
    check_required_files,
    run_polars_etl,
    prepare_druid_ingestion,
    check_druid_connectivity,
    load_to_postgresql,
    refresh_superset_datasets,
    setup_superset_dashboards
)

# ======================================================= CONFIGURACIÓN DEL DAG ======================================================

default_args = {
    'owner': 'vaers-polars-team',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'catchup': False
}

dag = DAG(
    'main_pipeline',
    default_args=default_args,
    description='Análisis de Efectos Adversos de Vacunas COVID-19 a partir de Datos del VAERS',
    schedule_interval=None,
    max_active_runs=1,
    tags=['vaers', 'covid', 'polars', 'druid', 'superset', 'healthcare', 'analytics']
)

# ======================================================= DEFINICIÓN DE TAREAS =======================================================

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

# 8. Ingestar datos chunked en Druid
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

# =========================================================== DEPENDENCIAS ===========================================================

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
