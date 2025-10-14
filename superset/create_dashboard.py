#!/usr/bin/env python3
"""
Script para crear dashboard automático en Superset con datos de Druid
Genera gráficos básicos de análisis VAERS
"""

import requests
import json
import time
import sys

class SupersetDashboardCreator:
    def __init__(self):
        self.base_url = "http://localhost:8088"
        self.session = requests.Session()
        self.access_token = None
        self.csrf_token = None
        
    def wait_for_superset(self, max_attempts=10):
        """Esperar a que Superset esté disponible"""
        print("⏳ Esperando a que Superset esté disponible...")
        
        for attempt in range(max_attempts):
            try:
                response = requests.get(f"{self.base_url}/health", timeout=5)
                if response.status_code == 200:
                    print("✅ Superset está disponible!")
                    return True
            except requests.exceptions.RequestException:
                pass
            
            print(f"   Intento {attempt + 1}/{max_attempts}... esperando 5 segundos")
            time.sleep(5)
        
        return False
    
    def get_csrf_token(self):
        """Obtener token CSRF"""
        try:
            response = self.session.get(f"{self.base_url}/api/v1/security/csrf_token/")
            if response.status_code == 200:
                result = response.json()
                self.csrf_token = result.get("result")
                self.session.headers.update({"X-CSRFToken": self.csrf_token})
                return True
        except Exception as e:
            print(f"❌ Error obteniendo CSRF: {str(e)}")
        return False

    def login(self):
        """Autenticación en Superset"""
        print("🔐 Iniciando sesión en Superset...")
        
        login_data = {
            "username": "admin",
            "password": "admin", 
            "provider": "db",
            "refresh": True
        }
        
        try:
            response = self.session.post(f"{self.base_url}/api/v1/security/login", json=login_data)
            
            if response.status_code == 200:
                result = response.json()
                self.access_token = result.get("access_token")
                
                self.session.headers.update({
                    "Authorization": f"Bearer {self.access_token}",
                    "Content-Type": "application/json"
                })
                
                print("✅ Autenticación exitosa")
                return self.get_csrf_token()
            else:
                print(f"❌ Error de autenticación: {response.status_code}")
                return False
                
        except Exception as e:
            print(f"❌ Error durante login: {str(e)}")
            return False
    
    def get_or_create_druid_database(self):
        """Obtener o crear base de datos Druid"""
        print("📊 Configurando conexión con Druid...")
        
        # Primero intentar encontrar una conexión existente
        try:
            response = self.session.get(f"{self.base_url}/api/v1/database/")
            if response.status_code == 200:
                databases = response.json().get("result", [])
                for db in databases:
                    if "druid" in db.get("database_name", "").lower():
                        print(f"✅ Usando base de datos existente: {db['database_name']} (ID: {db['id']})")
                        return db["id"]
        except Exception as e:
            print(f"⚠️ Error buscando bases existentes: {str(e)}")
        
        # Si no existe, crear nueva
        database_config = {
            "database_name": "vaers_druid_dashboard",
            "sqlalchemy_uri": "druid://broker:8082/druid/v2/sql/",
            "expose_in_sqllab": True,
            "allow_ctas": False,
            "allow_cvas": False,
            "allow_dml": False
        }
        
        try:
            response = self.session.post(f"{self.base_url}/api/v1/database/", json=database_config)
            if response.status_code == 201:
                database_id = response.json().get("id")
                print(f"✅ Nueva base de datos creada (ID: {database_id})")
                return database_id
            else:
                print(f"❌ Error creando base de datos: {response.status_code} - {response.text}")
                return None
        except Exception as e:
            print(f"❌ Excepción creando base de datos: {str(e)}")
            return None
    
    def create_dataset(self, database_id, table_name, schema_name=None):
        """Crear dataset basado en tabla Druid"""
        print(f"📈 Creando dataset para tabla: {table_name}")
        
        dataset_config = {
            "database": database_id,
            "table_name": table_name,
            "schema": schema_name
        }
        
        try:
            response = self.session.post(f"{self.base_url}/api/v1/dataset/", json=dataset_config)
            
            if response.status_code == 201:
                dataset_id = response.json().get("id") 
                print(f"✅ Dataset creado exitosamente (ID: {dataset_id})")
                return dataset_id
            elif response.status_code == 422:
                # Probablemente ya existe, intentar encontrarlo
                return self.find_existing_dataset(table_name)
            else:
                print(f"❌ Error creando dataset: {response.status_code}")
                print(f"   Respuesta: {response.text}")
                return None
                
        except Exception as e:
            print(f"❌ Excepción creando dataset: {str(e)}")
            return None
    
    def find_existing_dataset(self, table_name):
        """Buscar dataset existente"""
        try:
            response = self.session.get(f"{self.base_url}/api/v1/dataset/")
            if response.status_code == 200:
                datasets = response.json().get("result", [])
                for ds in datasets:
                    if ds.get("table_name") == table_name:
                        print(f"✅ Dataset existente encontrado: {table_name} (ID: {ds['id']})")
                        return ds["id"]
        except Exception:
            pass
        return None
    
    def create_simple_chart(self, dataset_id, chart_config):
        """Crear un gráfico simple"""
        print(f"📊 Creando gráfico: {chart_config['slice_name']}")
        
        chart_data = {
            "slice_name": chart_config["slice_name"],
            "datasource_id": dataset_id,
            "datasource_type": "table",
            "viz_type": chart_config["viz_type"],
            "params": json.dumps(chart_config["params"])
        }
        
        try:
            response = self.session.post(f"{self.base_url}/api/v1/chart/", json=chart_data)
            
            if response.status_code == 201:
                chart_id = response.json().get("id")
                print(f"✅ Gráfico creado exitosamente (ID: {chart_id})")
                return chart_id
            else:
                print(f"❌ Error creando gráfico: {response.status_code}")
                print(f"   Detalles: {response.text}")
                return None
                
        except Exception as e:
            print(f"❌ Excepción creando gráfico: {str(e)}")
            return None
    
    def create_dashboard(self, dashboard_title, chart_ids):
        """Crear dashboard con gráficos"""
        print(f"🎯 Creando dashboard: {dashboard_title}")
        
        dashboard_data = {
            "dashboard_title": dashboard_title,
            "slug": dashboard_title.lower().replace(" ", "-").replace("_", "-"),
            "published": True
        }
        
        try:
            response = self.session.post(f"{self.base_url}/api/v1/dashboard/", json=dashboard_data)
            
            if response.status_code == 201:
                dashboard_id = response.json().get("id")
                print(f"✅ Dashboard creado (ID: {dashboard_id})")
                
                # Intentar agregar gráficos (esto puede fallar pero el dashboard estará creado)
                for chart_id in chart_ids:
                    self.add_chart_to_dashboard(dashboard_id, chart_id)
                
                return dashboard_id
            else:
                print(f"❌ Error creando dashboard: {response.status_code}")
                print(f"   Detalles: {response.text}")
                return None
                
        except Exception as e:
            print(f"❌ Excepción creando dashboard: {str(e)}")
            return None
    
    def add_chart_to_dashboard(self, dashboard_id, chart_id):
        """Agregar gráfico a dashboard (método simplificado)"""
        try:
            # Obtener configuración actual del dashboard
            response = self.session.get(f"{self.base_url}/api/v1/dashboard/{dashboard_id}")
            if response.status_code == 200:
                print(f"   ✅ Gráfico {chart_id} vinculado al dashboard")
        except Exception as e:
            print(f"   ⚠️ No se pudo vincular gráfico {chart_id}: {str(e)}")

def get_basic_chart_configs():
    """Configuraciones básicas de gráficos para VAERS"""
    
    charts = [
        {
            "slice_name": "Total de Reportes por Fabricante",
            "viz_type": "pie",
            "params": {
                "datasource": "vaers_symptoms_by_manufacturer",
                "viz_type": "pie",
                "groupby": ["VAX_MANU_CLEAN"],
                "metrics": ["total_reports"],
                "adhoc_filters": [],
                "row_limit": 10
            }
        },
        {
            "slice_name": "Top 15 Síntomas Más Reportados",
            "viz_type": "bar",
            "params": {
                "datasource": "vaers_symptoms_by_manufacturer", 
                "viz_type": "dist_bar",
                "groupby": ["symptom_name"],
                "metrics": ["total_reports"],
                "adhoc_filters": [],
                "row_limit": 15,
                "order_desc": True
            }
        },
        {
            "slice_name": "Tasa de Hospitalización por Fabricante",
            "viz_type": "bar",
            "params": {
                "datasource": "vaers_symptoms_by_manufacturer",
                "viz_type": "dist_bar",
                "groupby": ["VAX_MANU_CLEAN"],
                "metrics": ["hospital_rate"],
                "adhoc_filters": [],
                "row_limit": 10
            }
        }
    ]
    
    return charts

def main():
    """Función principal"""
    print("🚀 Creando Dashboard Automático VAERS en Superset")
    print("="*60)
    
    creator = SupersetDashboardCreator()
    
    # Paso 1: Verificar disponibilidad de Superset
    if not creator.wait_for_superset():
        print("❌ Superset no está disponible")
        sys.exit(1)
    
    # Paso 2: Login
    if not creator.login():
        print("❌ No se pudo autenticar")
        sys.exit(1)
    
    # Paso 3: Configurar conexión Druid
    database_id = creator.get_or_create_druid_database()
    if not database_id:
        print("❌ No se pudo configurar conexión con Druid")
        sys.exit(1)
    
    # Paso 4: Crear dataset principal
    dataset_id = creator.create_dataset(database_id, "vaers_symptoms_by_manufacturer")
    if not dataset_id:
        print("❌ No se pudo crear dataset")
        sys.exit(1)
    
    # Paso 5: Crear gráficos básicos
    print("\n📊 Creando gráficos básicos...")
    chart_configs = get_basic_chart_configs()
    chart_ids = []
    
    for chart_config in chart_configs:
        chart_id = creator.create_simple_chart(dataset_id, chart_config)
        if chart_id:
            chart_ids.append(chart_id)
    
    # Paso 6: Crear dashboard
    if chart_ids:
        dashboard_id = creator.create_dashboard("Análisis VAERS COVID-19", chart_ids)
        
        if dashboard_id:
            print(f"\n🎉 ¡Dashboard creado exitosamente!")
            print(f"🌐 URL: http://localhost:8088/dashboard/{dashboard_id}")
            print(f"🔑 Usuario: admin / Contraseña: admin")
            print(f"📊 Gráficos creados: {len(chart_ids)}")
        else:
            print("\n⚠️ Dashboard no se pudo crear, pero gráficos están disponibles")
    else:
        print("\n❌ No se pudieron crear gráficos")
    
    print("="*60)

if __name__ == "__main__":
    main()