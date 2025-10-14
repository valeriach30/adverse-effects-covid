#!/usr/bin/env python3
"""
Dashboard Builder CORREGIDO - Con métricas y columnas correctas
"""

import json
import time
import urllib.request
import urllib.parse
import urllib.error
from http.cookiejar import CookieJar

def build_complete_vaers_dashboard():
    """Crear dashboard VAERS completo con todas las visualizaciones"""
    
    base_url = "http://localhost:8088"
    print("🚀 DASHBOARD BUILDER VAERS - VERSION CORREGIDA")
    print("="*55)
    
    # Verificar Superset
    print("⏳ Verificando Superset...")
    for i in range(10):
        try:
            req = urllib.request.Request(f"{base_url}/health")
            with urllib.request.urlopen(req, timeout=5) as response:
                if response.getcode() == 200:
                    print("✅ Superset disponible!")
                    break
        except Exception:
            print(f"   Intento {i+1}/10...")
            time.sleep(2)
    else:
        print("❌ Superset no disponible")
        return False
    
    # Setup cookies y autenticación
    cookie_jar = CookieJar()
    cookie_processor = urllib.request.HTTPCookieProcessor(cookie_jar)
    opener = urllib.request.build_opener(cookie_processor)
    urllib.request.install_opener(opener)
    
    # Login
    print("🔐 Autenticando...")
    login_data = json.dumps({
        "username": "admin",
        "password": "admin",
        "refresh": True,
        "provider": "db"
    }).encode('utf-8')
    
    login_headers = {
        'Content-Type': 'application/json',
        'Accept': 'application/json'
    }
    
    try:
        req = urllib.request.Request(f"{base_url}/api/v1/security/login", data=login_data, headers=login_headers)
        with urllib.request.urlopen(req, timeout=30) as response:
            result = json.loads(response.read().decode('utf-8'))
            access_token = result.get("access_token")
            
        if not access_token:
            print("❌ Error obteniendo token")
            return False
            
        print("✅ Autenticación exitosa!")
        
    except Exception as e:
        print(f"❌ Error en login: {str(e)}")
        return False
    
    # Headers autenticados
    auth_headers = {
        'Authorization': f'Bearer {access_token}',
        'Content-Type': 'application/json',
        'Accept': 'application/json'
    }
    
    # Obtener CSRF token
    print("🔑 Obteniendo token CSRF...")
    try:
        req = urllib.request.Request(f"{base_url}/api/v1/security/csrf_token/", headers=auth_headers)
        with urllib.request.urlopen(req, timeout=30) as response:
            csrf_result = json.loads(response.read().decode('utf-8'))
            csrf_token = csrf_result.get("result")
            
        auth_headers['X-CSRFToken'] = csrf_token
        print("✅ Token CSRF obtenido!")
        
    except Exception as e:
        print(f"❌ Error CSRF: {str(e)}")
        return False
    
    # Verificar datasets
    print("📊 Verificando datasets...")
    try:
        req = urllib.request.Request(f"{base_url}/api/v1/dataset/", headers=auth_headers)
        with urllib.request.urlopen(req, timeout=30) as response:
            datasets = json.loads(response.read().decode('utf-8')).get("result", [])
            
            datasets_info = {}
            for dataset in datasets:
                table_name = dataset.get("table_name", "")
                if "vaers" in table_name.lower():
                    datasets_info[table_name] = dataset["id"]
                    print(f"   ✅ {table_name} (ID: {dataset['id']})")
            
            if len(datasets_info) < 3:
                print("❌ Faltan datasets VAERS")
                return False
                
    except Exception as e:
        print(f"❌ Error verificando datasets: {str(e)}")
        return False
    
    # CREAR GRÁFICOS COMPLETOS
    print("\n📈 CREANDO GRÁFICOS VAERS...")
    print("="*40)
    
    chart_ids = []
    
    # 1. Gráfico de Fabricantes (usando dataset síntomas)
    if "vaers_symptoms_by_manufacturer" in datasets_info:
        dataset_id = datasets_info["vaers_symptoms_by_manufacturer"]
        
        chart_config = {
            "slice_name": "📊 Reportes por Fabricante de Vacuna",
            "datasource_id": dataset_id,
            "datasource_type": "table",
            "viz_type": "pie",
            "params": json.dumps({
                "datasource": f"{dataset_id}__table",
                "viz_type": "pie",
                "groupby": ["VAX_MANU_CLEAN"],
                "metrics": [
                    {
                        "aggregate": "SUM",
                        "column": {
                            "column_name": "total_reports",
                            "type": "BIGINT"
                        },
                        "expressionType": "SIMPLE",
                        "label": "Total Reportes"
                    }
                ],
                "adhoc_filters": [],
                "row_limit": 10000,
                "color_scheme": "supersetColors"
            })
        }
        
        print("📊 Creando gráfico de fabricantes...")
        try:
            data = json.dumps(chart_config).encode('utf-8')
            req = urllib.request.Request(f"{base_url}/api/v1/chart/", data=data, headers=auth_headers)
            req.get_method = lambda: 'POST'
            
            with urllib.request.urlopen(req, timeout=30) as response:
                if response.getcode() == 201:
                    chart_result = json.loads(response.read().decode('utf-8'))
                    chart_id = chart_result.get("id")
                    chart_ids.append(chart_id)
                    print(f"   ✅ Gráfico fabricantes creado (ID: {chart_id})")
                else:
                    print(f"   ❌ Error HTTP {response.getcode()}")
                    
        except Exception as e:
            print(f"   ❌ Error: {str(e)}")
    
    # 2. Gráfico de Top Síntomas
    if "vaers_symptoms_by_manufacturer" in datasets_info:
        dataset_id = datasets_info["vaers_symptoms_by_manufacturer"]
        
        chart_config = {
            "slice_name": "📈 Top 15 Síntomas Más Reportados",
            "datasource_id": dataset_id,
            "datasource_type": "table", 
            "viz_type": "dist_bar",
            "params": json.dumps({
                "datasource": f"{dataset_id}__table",
                "viz_type": "dist_bar",
                "groupby": ["symptom_name"],
                "metrics": [
                    {
                        "aggregate": "SUM",
                        "column": {
                            "column_name": "total_reports",
                            "type": "BIGINT"
                        },
                        "expressionType": "SIMPLE",
                        "label": "Total Reportes"
                    }
                ],
                "adhoc_filters": [],
                "row_limit": 15,
                "order_desc": True,
                "color_scheme": "supersetColors"
            })
        }
        
        print("📈 Creando gráfico de síntomas...")
        try:
            data = json.dumps(chart_config).encode('utf-8')
            req = urllib.request.Request(f"{base_url}/api/v1/chart/", data=data, headers=auth_headers)
            req.get_method = lambda: 'POST'
            
            with urllib.request.urlopen(req, timeout=30) as response:
                if response.getcode() == 201:
                    chart_result = json.loads(response.read().decode('utf-8'))
                    chart_id = chart_result.get("id")
                    chart_ids.append(chart_id)
                    print(f"   ✅ Gráfico síntomas creado (ID: {chart_id})")
                else:
                    print(f"   ❌ Error HTTP {response.getcode()}")
                    
        except Exception as e:
            print(f"   ❌ Error: {str(e)}")
    
    # 3. Gráfico de Hospitalizaciones por Edad
    if "vaers_severity_by_age" in datasets_info:
        dataset_id = datasets_info["vaers_severity_by_age"]
        
        chart_config = {
            "slice_name": "🏥 Hospitalizaciones por Grupo de Edad",
            "datasource_id": dataset_id,
            "datasource_type": "table",
            "viz_type": "dist_bar",
            "params": json.dumps({
                "datasource": f"{dataset_id}__table",
                "viz_type": "dist_bar", 
                "groupby": ["age_group"],
                "metrics": [
                    {
                        "aggregate": "SUM",
                        "column": {
                            "column_name": "hospitalizations",
                            "type": "BIGINT"
                        },
                        "expressionType": "SIMPLE",
                        "label": "Hospitalizaciones"
                    }
                ],
                "adhoc_filters": [],
                "row_limit": 50,
                "order_desc": True,
                "color_scheme": "supersetColors"
            })
        }
        
        print("🏥 Creando gráfico de hospitalizaciones...")
        try:
            data = json.dumps(chart_config).encode('utf-8')
            req = urllib.request.Request(f"{base_url}/api/v1/chart/", data=data, headers=auth_headers)
            req.get_method = lambda: 'POST'
            
            with urllib.request.urlopen(req, timeout=30) as response:
                if response.getcode() == 201:
                    chart_result = json.loads(response.read().decode('utf-8'))
                    chart_id = chart_result.get("id")
                    chart_ids.append(chart_id)
                    print(f"   ✅ Gráfico hospitalizaciones creado (ID: {chart_id})")
                else:
                    print(f"   ❌ Error HTTP {response.getcode()}")
                    
        except Exception as e:
            print(f"   ❌ Error: {str(e)}")
    
    # 4. Mapa de Estados (Distribución Geográfica)
    if "vaers_geographic_distribution" in datasets_info:
        dataset_id = datasets_info["vaers_geographic_distribution"]
        
        chart_config = {
            "slice_name": "🗺️ Distribución Geográfica por Estado",
            "datasource_id": dataset_id,
            "datasource_type": "table",
            "viz_type": "dist_bar",
            "params": json.dumps({
                "datasource": f"{dataset_id}__table",
                "viz_type": "dist_bar",
                "groupby": ["state"],
                "metrics": [
                    {
                        "aggregate": "SUM", 
                        "column": {
                            "column_name": "total_reports",
                            "type": "BIGINT"
                        },
                        "expressionType": "SIMPLE",
                        "label": "Total Reportes"
                    }
                ],
                "adhoc_filters": [],
                "row_limit": 20,
                "order_desc": True,
                "color_scheme": "supersetColors"
            })
        }
        
        print("🗺️ Creando gráfico geográfico...")
        try:
            data = json.dumps(chart_config).encode('utf-8')
            req = urllib.request.Request(f"{base_url}/api/v1/chart/", data=data, headers=auth_headers)
            req.get_method = lambda: 'POST'
            
            with urllib.request.urlopen(req, timeout=30) as response:
                if response.getcode() == 201:
                    chart_result = json.loads(response.read().decode('utf-8'))
                    chart_id = chart_result.get("id")
                    chart_ids.append(chart_id)
                    print(f"   ✅ Gráfico geográfico creado (ID: {chart_id})")
                else:
                    print(f"   ❌ Error HTTP {response.getcode()}")
                    
        except Exception as e:
            print(f"   ❌ Error: {str(e)}")
    
    # CREAR DASHBOARD COMPLETO
    if chart_ids:
        print(f"\n🎯 CREANDO DASHBOARD CON {len(chart_ids)} GRÁFICOS...")
        print("="*50)
        
        # Layout con posiciones específicas
        position_json = {}
        
        # Organizar gráficos en grid 2x2
        positions = [
            {"x": 0, "y": 0, "w": 6, "h": 8},   # Fabricantes (arriba izquierda)
            {"x": 6, "y": 0, "w": 6, "h": 8},   # Síntomas (arriba derecha)  
            {"x": 0, "y": 8, "w": 6, "h": 8},   # Hospitalizaciones (abajo izquierda)
            {"x": 6, "y": 8, "w": 6, "h": 8}    # Geográfico (abajo derecha)
        ]
        
        for i, chart_id in enumerate(chart_ids):
            if i < len(positions):
                pos = positions[i]
                position_json[f"CHART-{chart_id}"] = {
                    "children": [],
                    "id": f"CHART-{chart_id}",
                    "meta": {
                        "chartId": chart_id,
                        "width": pos["w"],
                        "height": pos["h"]
                    },
                    "parents": ["ROOT_ID"],
                    "type": "CHART",
                    "x": pos["x"],
                    "y": pos["y"],
                    "w": pos["w"], 
                    "h": pos["h"]
                }
        
        dashboard_data = {
            "dashboard_title": "📊 VAERS COVID-19 - Dashboard Completo Automatizado",
            "published": True,
            "position_json": json.dumps(position_json)
        }
        
        try:
            data = json.dumps(dashboard_data).encode('utf-8')
            req = urllib.request.Request(f"{base_url}/api/v1/dashboard/", data=data, headers=auth_headers)
            req.get_method = lambda: 'POST'
            
            with urllib.request.urlopen(req, timeout=30) as response:
                if response.getcode() == 201:
                    dashboard_result = json.loads(response.read().decode('utf-8'))
                    dashboard_id = dashboard_result.get("id")
                    print(f"✅ DASHBOARD CREADO EXITOSAMENTE! (ID: {dashboard_id})")
                    print(f"🔗 URL: {base_url}/superset/dashboard/{dashboard_id}/")
                else:
                    print(f"❌ Error HTTP {response.getcode()} creando dashboard")
                    
        except Exception as e:
            print(f"❌ Error creando dashboard: {str(e)}")
    
    # RESUMEN FINAL
    print("\n" + "🎉" + "="*53 + "🎉")
    print("           DASHBOARD VAERS COMPLETO FINALIZADO")
    print("="*55)
    print(f"📊 Total gráficos creados: {len(chart_ids)}")
    print("📋 Gráficos incluidos:")
    print("   • Distribución por fabricantes de vacunas")
    print("   • Top síntomas más reportados") 
    print("   • Hospitalizaciones por grupo de edad")
    print("   • Distribución geográfica por estados")
    print("="*55)
    print(f"🌐 Acceso: {base_url}")
    print("🔑 Login: admin / admin")
    print("="*55)
    
    return len(chart_ids) > 0

if __name__ == "__main__":
    success = build_complete_vaers_dashboard()
    if success:
        print("✅ ¡DASHBOARD COMPLETO CREADO EXITOSAMENTE!")
    else:
        print("❌ Error creando dashboard completo")