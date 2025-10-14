#!/usr/bin/env python3
"""
Script para configurar conexiones y dashboards en Superset
Conecta Superset con Druid para visualizar análisis VAERS
"""

import requests
import json
import time

# Configuración de Superset
SUPERSET_URL = "http://localhost:8088"
SUPERSET_USERNAME = "admin"
SUPERSET_PASSWORD = "admin"

# Configuración de Druid
DRUID_URL = "http://broker:8082"

class SupersetConfigurator:
    def __init__(self):
        self.session = requests.Session()
        self.access_token = None
        
    def login(self):
        """Autenticación en Superset"""
        login_data = {
            "username": SUPERSET_USERNAME,
            "password": SUPERSET_PASSWORD,
            "refresh": True
        }
        
        response = self.session.post(f"{SUPERSET_URL}/api/v1/security/login", json=login_data)
        if response.status_code == 200:
            self.access_token = response.json()["access_token"]
            self.session.headers.update({"Authorization": f"Bearer {self.access_token}"})
            print("✅ Autenticación exitosa en Superset")
            return True
        else:
            print(f"❌ Error de autenticación: {response.text}")
            return False
    
    def create_druid_database(self):
        """Crea conexión de base de datos Druid"""
        database_data = {
            "database_name": "vaers_druid",
            "sqlalchemy_uri": "druid://broker:8082/druid/v2/sql",
            "expose_in_sqllab": True,
            "allow_ctas": False,
            "allow_cvas": False,
            "allow_dml": False
        }
        
        response = self.session.post(f"{SUPERSET_URL}/api/v1/database/", json=database_data)
        if response.status_code == 201:
            db_id = response.json()["id"]
            print(f"✅ Base de datos Druid creada con ID: {db_id}")
            return db_id
        else:
            print(f"❌ Error creando base de datos: {response.text}")
            return None
    
    def create_dataset(self, database_id, table_name):
        """Crea dataset basado en tabla Druid"""
        dataset_data = {
            "database": database_id,
            "table_name": table_name,
            "sql": f"SELECT * FROM {table_name}"
        }
        
        response = self.session.post(f"{SUPERSET_URL}/api/v1/dataset/", json=dataset_data)
        if response.status_code == 201:
            dataset_id = response.json()["id"]
            print(f"✅ Dataset '{table_name}' creado con ID: {dataset_id}")
            return dataset_id
        else:
            print(f"❌ Error creando dataset {table_name}: {response.text}")
            return None
    
    def create_chart(self, dataset_id, chart_config):
        """Crea gráfico en Superset"""
        response = self.session.post(f"{SUPERSET_URL}/api/v1/chart/", json=chart_config)
        if response.status_code == 201:
            chart_id = response.json()["id"]
            print(f"✅ Gráfico '{chart_config['slice_name']}' creado con ID: {chart_id}")
            return chart_id
        else:
            print(f"❌ Error creando gráfico: {response.text}")
            return None
    
    def create_dashboard(self, dashboard_name, chart_ids):
        """Crea dashboard con gráficos"""
        dashboard_data = {
            "dashboard_title": dashboard_name,
            "slug": dashboard_name.lower().replace(" ", "-"),
            "published": True
        }
        
        response = self.session.post(f"{SUPERSET_URL}/api/v1/dashboard/", json=dashboard_data)
        if response.status_code == 201:
            dashboard_id = response.json()["id"]
            print(f"✅ Dashboard '{dashboard_name}' creado con ID: {dashboard_id}")
            
            # Agregar gráficos al dashboard
            for chart_id in chart_ids:
                self.add_chart_to_dashboard(dashboard_id, chart_id)
            
            return dashboard_id
        else:
            print(f"❌ Error creando dashboard: {response.text}")
            return None
    
    def add_chart_to_dashboard(self, dashboard_id, chart_id):
        """Agrega gráfico a dashboard"""
        # Obtener configuración actual del dashboard
        response = self.session.get(f"{SUPERSET_URL}/api/v1/dashboard/{dashboard_id}")
        if response.status_code == 200:
            dashboard_data = response.json()["result"]
            
            # Agregar gráfico a la configuración
            if "position_json" not in dashboard_data:
                dashboard_data["position_json"] = "{}"
            
            # Actualizar dashboard
            update_data = {
                "position_json": dashboard_data["position_json"]
            }
            
            response = self.session.put(f"{SUPERSET_URL}/api/v1/dashboard/{dashboard_id}", json=update_data)
            if response.status_code == 200:
                print(f"✅ Gráfico {chart_id} agregado al dashboard {dashboard_id}")
            else:
                print(f"❌ Error agregando gráfico al dashboard: {response.text}")

def create_vaers_charts_config():
    """Configuraciones de gráficos para análisis VAERS"""
    
    charts = [
        {
            "slice_name": "Top 10 Síntomas por Fabricante",
            "viz_type": "table",
            "params": {
                "datasource": "vaers_symptoms_by_manufacturer",
                "viz_type": "table",
                "metrics": ["total_reports", "deaths", "hospitalizations"],
                "groupby": ["VAX_MANU_CLEAN", "symptom_name"],
                "row_limit": 50,
                "order_desc": True,
                "table_timestamp_format": "%Y-%m-%d %H:%M:%S"
            }
        },
        {
            "slice_name": "Reportes por Fabricante",
            "viz_type": "pie",
            "params": {
                "datasource": "vaers_symptoms_by_manufacturer",
                "viz_type": "pie",
                "metrics": ["total_reports"],
                "groupby": ["VAX_MANU_CLEAN"],
                "row_limit": 10
            }
        },
        {
            "slice_name": "Tasa de Hospitalización por Síntoma",
            "viz_type": "bar",
            "params": {
                "datasource": "vaers_symptoms_by_manufacturer", 
                "viz_type": "dist_bar",
                "metrics": ["hospital_rate"],
                "groupby": ["symptom_name"],
                "row_limit": 20,
                "order_desc": True
            }
        },
        {
            "slice_name": "Comparación de Severidad por Fabricante",
            "viz_type": "bar",
            "params": {
                "datasource": "vaers_symptoms_by_manufacturer",
                "viz_type": "dist_bar", 
                "metrics": ["death_rate", "hospital_rate"],
                "groupby": ["VAX_MANU_CLEAN"],
                "row_limit": 10
            }
        }
    ]
    
    return charts

def main():
    """Función principal para configurar Superset"""
    print("🚀 Configurando Superset para análisis VAERS...")
    
    configurator = SupersetConfigurator()
    
    # Esperar a que Superset esté listo
    print("⏳ Esperando a que Superset esté disponible...")
    time.sleep(60)
    
    # Autenticación
    if not configurator.login():
        return
    
    # Crear conexión a Druid
    print("\n📊 Configurando conexión con Druid...")
    database_id = configurator.create_druid_database()
    if not database_id:
        return
    
    # Crear datasets
    print("\n📈 Creando datasets...")
    dataset_id = configurator.create_dataset(database_id, "vaers_symptoms_by_manufacturer")
    if not dataset_id:
        return
    
    # Crear gráficos
    print("\n📊 Creando gráficos...")
    charts_config = create_vaers_charts_config()
    chart_ids = []
    
    for chart_config in charts_config:
        chart_config["datasource_id"] = dataset_id
        chart_config["datasource_type"] = "table"
        
        chart_id = configurator.create_chart(dataset_id, chart_config)
        if chart_id:
            chart_ids.append(chart_id)
    
    # Crear dashboard
    print("\n🎯 Creando dashboard...")
    dashboard_id = configurator.create_dashboard("Análisis VAERS COVID-19", chart_ids)
    
    if dashboard_id:
        print(f"""
✅ Configuración completada exitosamente!

🌐 Acceso a los dashboards:
   - Dashboard VAERS: {SUPERSET_URL}/dashboard/{dashboard_id}
   - Lista de dashboards: {SUPERSET_URL}/dashboard/list/
   
📊 Credenciales:
   - Usuario: {SUPERSET_USERNAME}
   - Contraseña: {SUPERSET_PASSWORD}
   
🔗 URLs útiles:
   - Superset: {SUPERSET_URL}
   - Druid Console: http://localhost:8888
   - Airflow: http://localhost:8080
        """)
    
if __name__ == "__main__":
    main()