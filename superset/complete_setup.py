#!/usr/bin/env python3
"""
Script final para configuración completa automática de Superset
Crea conexiones, datasets y dashboards para todos los análisis VAERS
"""

import requests
import json
import time

class CompleteSuperset:
    def __init__(self):
        self.base_url = "http://localhost:8088"
        self.session = requests.Session()
        self.database_id = None
        
    def setup_complete_dashboard(self):
        """Configuración completa automática"""
        print("🚀 Configuración completa automática de Superset")
        print("="*60)
        
        # Autenticación
        if not self._authenticate():
            raise Exception("No se pudo autenticar")
        
        # Configurar base de datos
        self.database_id = self._setup_database()
        if not self.database_id:
            raise Exception("No se pudo configurar base de datos")
        
        # Crear datasets para todas las tablas
        datasets = self._create_all_datasets()
        
        # Crear gráficos avanzados
        charts = self._create_advanced_charts(datasets)
        
        # Crear dashboard completo
        dashboard_id = self._create_comprehensive_dashboard(charts)
        
        # Reporte final
        self._final_report(dashboard_id, len(datasets), len(charts))
        
        return dashboard_id
    
    def _authenticate(self):
        """Autenticación completa"""
        print("🔐 Autenticando en Superset...")
        
        # Esperar disponibilidad
        for attempt in range(5):
            try:
                response = requests.get(f"{self.base_url}/health", timeout=5)
                if response.status_code == 200:
                    break
            except:
                time.sleep(5)
        
        # Login
        login_data = {
            "username": "admin", 
            "password": "admin",
            "provider": "db",
            "refresh": True
        }
        
        response = self.session.post(f"{self.base_url}/api/v1/security/login", json=login_data)
        if response.status_code != 200:
            return False
        
        access_token = response.json().get("access_token")
        self.session.headers.update({
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json"
        })
        
        # CSRF Token
        csrf_response = self.session.get(f"{self.base_url}/api/v1/security/csrf_token/")
        csrf_token = csrf_response.json().get("result")
        self.session.headers.update({"X-CSRFToken": csrf_token})
        
        print("✅ Autenticación exitosa")
        return True
    
    def _setup_database(self):
        """Configurar base de datos Druid"""
        print("📊 Configurando conexión con Druid...")
        
        # Verificar si existe
        response = self.session.get(f"{self.base_url}/api/v1/database/")
        databases = response.json().get("result", [])
        
        for db in databases:
            if "druid" in db.get("database_name", "").lower():
                print(f"✅ Usando base de datos existente: {db['database_name']}")
                return db["id"]
        
        # Crear nueva
        database_config = {
            "database_name": "VAERS_Druid_Analytics",
            "sqlalchemy_uri": "druid://broker:8082/druid/v2/sql/",
            "expose_in_sqllab": True,
            "allow_ctas": False,
            "allow_cvas": False
        }
        
        response = self.session.post(f"{self.base_url}/api/v1/database/", json=database_config)
        if response.status_code == 201:
            db_id = response.json().get("id")
            print(f"✅ Nueva base de datos creada (ID: {db_id})")
            return db_id
        
        return None
    
    def _create_all_datasets(self):
        """Crear datasets para todas las tablas de análisis"""
        print("📈 Creando datasets para todas las tablas...")
        
        tables = [
            "vaers_symptoms_by_manufacturer",
            "vaers_severity_by_age",
            "vaers_geographic_distribution"
        ]
        
        datasets = {}
        
        for table in tables:
            dataset_config = {
                "database": self.database_id,
                "table_name": table
            }
            
            try:
                response = self.session.post(f"{self.base_url}/api/v1/dataset/", json=dataset_config)
                
                if response.status_code == 201:
                    dataset_id = response.json().get("id")
                    datasets[table] = dataset_id
                    print(f"✅ Dataset creado: {table} (ID: {dataset_id})")
                elif response.status_code == 422:
                    # Ya existe, buscar ID
                    existing_id = self._find_dataset_id(table)
                    if existing_id:
                        datasets[table] = existing_id
                        print(f"✅ Dataset existente: {table} (ID: {existing_id})")
                
            except Exception as e:
                print(f"⚠️ Error con dataset {table}: {str(e)}")
        
        return datasets
    
    def _find_dataset_id(self, table_name):
        """Buscar ID de dataset existente"""
        try:
            response = self.session.get(f"{self.base_url}/api/v1/dataset/")
            datasets = response.json().get("result", [])
            
            for ds in datasets:
                if ds.get("table_name") == table_name:
                    return ds["id"]
        except:
            pass
        return None
    
    def _create_advanced_charts(self, datasets):
        """Crear gráficos avanzados para cada dataset"""
        print("📊 Creando gráficos avanzados...")
        
        charts = []
        
        # Gráficos para síntomas por fabricante
        if "vaers_symptoms_by_manufacturer" in datasets:
            dataset_id = datasets["vaers_symptoms_by_manufacturer"]
            
            chart_configs = [
                {
                    "slice_name": "📊 Distribución por Fabricante",
                    "viz_type": "pie",
                    "params": {
                        "groupby": ["VAX_MANU_CLEAN"],
                        "metrics": ["total_reports"],
                        "row_limit": 10
                    }
                },
                {
                    "slice_name": "📈 Top Síntomas Reportados",
                    "viz_type": "bar", 
                    "params": {
                        "groupby": ["symptom_name"],
                        "metrics": ["total_reports"],
                        "row_limit": 20,
                        "order_desc": True
                    }
                },
                {
                    "slice_name": "⚕️ Tasas de Complicaciones",
                    "viz_type": "bar",
                    "params": {
                        "groupby": ["VAX_MANU_CLEAN"],
                        "metrics": ["death_rate", "hospital_rate"],
                        "row_limit": 10
                    }
                }
            ]
            
            for config in chart_configs:
                chart_id = self._create_chart(dataset_id, config)
                if chart_id:
                    charts.append(chart_id)
        
        # Gráficos para severidad por edad
        if "vaers_severity_by_age" in datasets:
            dataset_id = datasets["vaers_severity_by_age"]
            
            age_charts = [
                {
                    "slice_name": "👥 Análisis por Grupo de Edad",
                    "viz_type": "bar",
                    "params": {
                        "groupby": ["age_group"],
                        "metrics": ["total_cases"],
                        "row_limit": 10
                    }
                }
            ]
            
            for config in age_charts:
                chart_id = self._create_chart(dataset_id, config)
                if chart_id:
                    charts.append(chart_id)
        
        # Gráfico geográfico
        if "vaers_geographic_distribution" in datasets:
            dataset_id = datasets["vaers_geographic_distribution"]
            
            geo_charts = [
                {
                    "slice_name": "🗺️ Distribución por Estado",
                    "viz_type": "bar",
                    "params": {
                        "groupby": ["state"],
                        "metrics": ["total_reports"],
                        "row_limit": 15,
                        "order_desc": True
                    }
                }
            ]
            
            for config in geo_charts:
                chart_id = self._create_chart(dataset_id, config)
                if chart_id:
                    charts.append(chart_id)
        
        print(f"✅ {len(charts)} gráficos creados exitosamente")
        return charts
    
    def _create_chart(self, dataset_id, config):
        """Crear un gráfico individual"""
        chart_data = {
            "slice_name": config["slice_name"],
            "datasource_id": dataset_id,
            "datasource_type": "table",
            "viz_type": config["viz_type"],
            "params": json.dumps(config["params"])
        }
        
        try:
            response = self.session.post(f"{self.base_url}/api/v1/chart/", json=chart_data)
            if response.status_code == 201:
                chart_id = response.json().get("id")
                print(f"   ✅ {config['slice_name']} (ID: {chart_id})")
                return chart_id
        except Exception as e:
            print(f"   ⚠️ Error creando {config['slice_name']}: {str(e)}")
        
        return None
    
    def _create_comprehensive_dashboard(self, chart_ids):
        """Crear dashboard completo"""
        print("🎯 Creando dashboard completo...")
        
        dashboard_data = {
            "dashboard_title": "🏥 VAERS COVID-19 - Análisis Completo",
            "slug": "vaers-covid19-analisis-completo",
            "published": True
        }
        
        try:
            response = self.session.post(f"{self.base_url}/api/v1/dashboard/", json=dashboard_data)
            
            if response.status_code == 201:
                dashboard_id = response.json().get("id")
                print(f"✅ Dashboard completo creado (ID: {dashboard_id})")
                return dashboard_id
            elif response.status_code == 422:
                # Ya existe, buscar ID
                existing_id = self._find_dashboard_by_title("VAERS COVID-19")
                if existing_id:
                    print(f"✅ Dashboard existente (ID: {existing_id})")
                    return existing_id
        except Exception as e:
            print(f"⚠️ Error creando dashboard: {str(e)}")
        
        return None
    
    def _find_dashboard_by_title(self, title_part):
        """Buscar dashboard por título parcial"""
        try:
            response = self.session.get(f"{self.base_url}/api/v1/dashboard/")
            dashboards = response.json().get("result", [])
            
            for dash in dashboards:
                if title_part.lower() in dash.get("dashboard_title", "").lower():
                    return dash["id"]
        except:
            pass
        return None
    
    def _final_report(self, dashboard_id, num_datasets, num_charts):
        """Reporte final de configuración"""
        print("\n" + "🎉" + "="*58 + "🎉")
        print("  CONFIGURACIÓN AUTOMÁTICA COMPLETADA EXITOSAMENTE")
        print("="*60)
        print(f"📊 Datasets creados: {num_datasets}")
        print(f"📈 Gráficos creados: {num_charts}")
        print(f"🎯 Dashboard ID: {dashboard_id}")
        print("")
        print("🌐 ACCESO A SUPERSET:")
        print(f"   URL Principal: {self.base_url}")
        print("   Usuario: admin")
        print("   Contraseña: admin")
        print("")
        if dashboard_id:
            print(f"📊 DASHBOARD PRINCIPAL:")
            print(f"   {self.base_url}/dashboard/{dashboard_id}")
        print("")
        print("📋 OTROS RECURSOS:")
        print(f"   Lista de Dashboards: {self.base_url}/dashboard/list/")
        print(f"   SQL Lab: {self.base_url}/sqllab/")
        print(f"   Datasets: {self.base_url}/tablemodelview/list/")
        print("="*60)

def main():
    """Función principal"""
    try:
        configurator = CompleteSuperset()
        dashboard_id = configurator.setup_complete_dashboard()
        
        if dashboard_id:
            print("\n✨ ¡Configuración exitosa! Dashboard disponible en Superset")
            return True
        else:
            print("\n⚠️ Configuración parcial - algunos elementos pueden no haberse creado")
            return False
            
    except Exception as e:
        print(f"\n❌ Error en configuración: {str(e)}")
        return False

if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)