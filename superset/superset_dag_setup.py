#!/usr/bin/env python3
"""
Script de configuración automática de Superset para el DAG de Airflow
Se ejecuta después de que los datos estén en Druid
"""

import sys
import os

# Agregar el directorio superset al path para importar módulos
sys.path.append('/opt/airflow/superset')

from superset.create_dashboard import SupersetDashboardCreator, get_basic_chart_configs

def setup_superset_dashboard():
    """Función para usar en el DAG de Airflow"""
    print("🎯 Configurando dashboard de Superset automáticamente...")
    
    creator = SupersetDashboardCreator()
    
    # Configurar conexión y crear dashboard
    if not creator.wait_for_superset(max_attempts=15):
        raise Exception("Superset no está disponible")
    
    if not creator.login():
        raise Exception("No se pudo autenticar en Superset")
    
    database_id = creator.get_or_create_druid_database()
    if not database_id:
        raise Exception("No se pudo configurar conexión con Druid")
    
    # Crear datasets para todas las tablas de análisis
    datasets = {}
    tables = [
        "vaers_symptoms_by_manufacturer",
        "vaers_severity_by_age", 
        "vaers_geographic_distribution"
    ]
    
    for table in tables:
        dataset_id = creator.create_dataset(database_id, table)
        if dataset_id:
            datasets[table] = dataset_id
            print(f"✅ Dataset creado para {table}")
        else:
            print(f"⚠️ No se pudo crear dataset para {table}")
    
    if not datasets:
        raise Exception("No se pudieron crear datasets")
    
    # Crear gráficos
    chart_ids = []
    chart_configs = get_basic_chart_configs()
    
    # Usar dataset de síntomas por fabricante para gráficos básicos
    main_dataset_id = datasets.get("vaers_symptoms_by_manufacturer")
    if main_dataset_id:
        for chart_config in chart_configs:
            chart_id = creator.create_simple_chart(main_dataset_id, chart_config)
            if chart_id:
                chart_ids.append(chart_id)
    
    # Crear dashboard
    if chart_ids:
        dashboard_id = creator.create_dashboard("VAERS COVID-19 Análisis Automático", chart_ids)
        if dashboard_id:
            print(f"🎉 Dashboard automático creado exitosamente!")
            print(f"📊 URL: http://localhost:8088/dashboard/{dashboard_id}")
            return f"Dashboard creado exitosamente con {len(chart_ids)} gráficos"
        else:
            raise Exception("No se pudo crear dashboard")
    else:
        raise Exception("No se pudieron crear gráficos")

if __name__ == "__main__":
    try:
        result = setup_superset_dashboard()
        print(result)
    except Exception as e:
        print(f"❌ Error: {str(e)}")
        sys.exit(1)