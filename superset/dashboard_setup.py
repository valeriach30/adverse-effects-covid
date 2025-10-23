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
    
    # Detectar si estamos corriendo en Docker/Airflow
    import os
    if os.path.exists('/opt/airflow') or os.environ.get('AIRFLOW_HOME'):
        # Ejecutándose desde contenedor Airflow - usar nombre del contenedor
        base_url = "http://superset:8088"
        print("🐳 Ejecutándose desde contenedor Docker/Airflow")
    else:
        # Ejecutándose desde host local
        base_url = "http://localhost:8088"
        print("💻 Ejecutándose desde host local")
    
    print("🚀 DASHBOARD BUILDER VAERS - VERSION CORREGIDA")
    print("="*55)
    print(f"🌐 URL Superset: {base_url}")
    
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
    
    # ===================================================================
    # PASO 1: CREAR EL DASHBOARD (VACÍO) PRIMERO
    # ===================================================================
    print("\n🎯 PASO 1: CREANDO DASHBOARD BASE...")
    dashboard_id = None
    dashboard_title = "📊 VAERS COVID-19 - Dashboard Completo Automatizado"
    
    dashboard_create_data = {
        "dashboard_title": dashboard_title,
        "published": True
    }
    
    try:
        data = json.dumps(dashboard_create_data).encode('utf-8')
        req = urllib.request.Request(f"{base_url}/api/v1/dashboard/", data=data, headers=auth_headers)
        req.get_method = lambda: 'POST'
        
        with urllib.request.urlopen(req, timeout=30) as response:
            if response.getcode() == 201:
                dashboard_result = json.loads(response.read().decode('utf-8'))
                dashboard_id = dashboard_result.get("id")
                print(f"   ✅ Dashboard base creado (ID: {dashboard_id})")
            else:
                print(f"   ❌ Error HTTP {response.getcode()} creando dashboard base")
    except Exception as e:
        print(f"   ❌ Error creando dashboard base: {str(e)}")
        return False

    # Si falló la creación del dashboard, salimos
    if not dashboard_id:
        print("❌ No se pudo crear el dashboard. Abortando.")
        return False

    # ===================================================================
    # PASO 2: CREAR GRÁFICOS Y ASOCIARLOS AL DASHBOARD
    # ===================================================================
    print("\n📈 PASO 2: CREANDO GRÁFICOS VAERS...")
    print("="*40)
    
    chart_ids = []
    
        # 1. Chart: Distribución por Fabricante (PIE)
    if "vaers_symptoms_analysis" in datasets_info:
        dataset_id = datasets_info["vaers_symptoms_analysis"]
        
        chart_config = {
            "slice_name": "📊 Reportes por Fabricante de Vacuna",
            "datasource_id": dataset_id,
            "datasource_type": "table",
            "viz_type": "pie",
            "dashboards": [dashboard_id],
            "params": json.dumps({
                "viz_type": "pie",
                "groupby": ["manufacturer"],
                "metric": { 
                    "aggregate": "SUM",
                    "column": {"column_name": "total_reports", "type": "BIGINT"},
                    "expressionType": "SIMPLE",
                    "label": "Total Reportes",
                    "hasCustomLabel": True
                },
                "adhoc_filters": [],
                "row_limit": 10000,
                "color_scheme": "supersetColors",
                "sort_by_metric": True
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
                    print(f"   ✅ Gráfico fabricantes creado y asociado (ID: {chart_id})")
                else:
                    print(f"   ❌ Error HTTP {response.getcode()}")
                    
        except Exception as e:
            print(f"   ❌ Error: {str(e)}")
    
    # 2. Gráfico de Top Síntomas
    if "vaers_symptoms_analysis" in datasets_info:
        dataset_id = datasets_info["vaers_symptoms_analysis"]
        
        chart_config = {
            "slice_name": "📈 Top 15 Síntomas Más Reportados",
            "datasource_id": dataset_id,
            "datasource_type": "table", 
            "viz_type": "dist_bar",
            "dashboards": [dashboard_id],
            "params": json.dumps({
                "viz_type": "dist_bar",
                "groupby": ["symptom"],
                "metrics": [
                    {
                        "aggregate": "SUM",
                        "column": {"column_name": "total_reports", "type": "BIGINT"},
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
                    print(f"   ✅ Gráfico síntomas creado y asociado (ID: {chart_id})")
        except Exception as e:
            print(f"   ❌ Error: {str(e)}")

    # 3. Gráfico de Hospitalizaciones por Edad  
    if "vaers_severity_analysis" in datasets_info:
        dataset_id = datasets_info["vaers_severity_analysis"]
        chart_config = {
            "slice_name": "🏥 Hospitalizaciones por Grupo de Edad",
            "datasource_id": dataset_id,
            "datasource_type": "table",
            "viz_type": "dist_bar",
            "dashboards": [dashboard_id],
            "params": json.dumps({
                "viz_type": "dist_bar", 
                "groupby": ["age_group"],
                "metrics": [
                    {
                        "aggregate": "SUM",
                        "column": {"column_name": "hospitalizations", "type": "BIGINT"},
                        "expressionType": "SIMPLE",
                        "label": "Hospitalizaciones"
                    }
                ],
                "row_limit": 50,
                "sort_x_axis": "descending",
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
                    print(f"   ✅ Gráfico hospitalizaciones creado y asociado (ID: {chart_id})")
        except Exception as e:
            print(f"   ❌ Error: {str(e)}")
    
    # 4. Mapa de Estados (Distribución Geográfica)
    if "vaers_geographic_analysis" in datasets_info:
        dataset_id = datasets_info["vaers_geographic_analysis"]
        chart_config = {
            "slice_name": "🗺️ Distribución Geográfica por Estado",
            "datasource_id": dataset_id,
            "datasource_type": "table",
            "viz_type": "dist_bar",
            "dashboards": [dashboard_id],
            "params": json.dumps({
                "viz_type": "dist_bar",
                "groupby": ["state"],
                "metrics": [
                    {
                        "aggregate": "SUM", 
                        "column": {"column_name": "total_reports", "type": "BIGINT"},
                        "expressionType": "SIMPLE",
                        "label": "Total Reportes"
                    }
                ],
                "row_limit": 20
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
                    print(f"   ✅ Gráfico geográfico creado y asociado (ID: {chart_id})")
        except Exception as e:
            print(f"   ❌ Error: {str(e)}")

    # ===================================================================
    # PASO 3: ACTUALIZAR EL DASHBOARD CON EL LAYOUT (position_json)
    # ===================================================================
    if chart_ids and dashboard_id: 
        print(f"\n🎨 PASO 3: APLICANDO LAYOUT FINAL AL DASHBOARD {dashboard_id}...")
        
        # 1. Definir IDs
        root_id = "ROOT_ID"
        tabs_id = "TABS_ID"
        tab_id = "TAB_ID_1"
        row_1_id = "ROW_ID_1"
        row_2_id = "ROW_ID_2" 

        # 2. Inicializar el position_json
        position_json = {
            root_id: {
                "type": "ROOT",
                "id": root_id,
                "children": [tabs_id]
            },
            tabs_id: {
                "type": "TABS",
                "id": tabs_id,
                "parents": [root_id],
                "children": [tab_id]
            },
            tab_id: {
                "type": "TAB",
                "id": tab_id,
                "parents": [tabs_id],
                "meta": {
                    "text": "Pestaña 1",
                    "background": "BACKGROUND_TRANSPARENT"
                },
                "children": [row_1_id, row_2_id] 
            },
            
            row_1_id: {
                "type": "ROW",
                "id": row_1_id,
                "parents": [tab_id],
                "children": [], # se llenará con gráficos 1 y 2
                "meta": {
                    "background": "BACKGROUND_TRANSPARENT"
                }
            },
            row_2_id: {
                "type": "ROW",
                "id": row_2_id,
                "parents": [tab_id],
                "children": [], # se llenará con gráficos 3 y 4
                "meta": {
                    "background": "BACKGROUND_TRANSPARENT"
                }
            }
        }
        
        # 3. Mapear los chart_ids (Esta lógica estaba perfecta)
        
        if len(chart_ids) >= 1: # Gráfico 1 (Fabricantes)
            chart_layout_id = f"CHART-{chart_ids[0]}"
            position_json[row_1_id]["children"].append(chart_layout_id)
            position_json[chart_layout_id] = {
                "type": "CHART", "id": chart_layout_id, "parents": [row_1_id],
                "meta": {"chartId": chart_ids[0], "width": 6, "height": 50} 
            }
            
        if len(chart_ids) >= 2: # Gráfico 2 (Síntomas)
            chart_layout_id = f"CHART-{chart_ids[1]}"
            position_json[row_1_id]["children"].append(chart_layout_id)
            position_json[chart_layout_id] = {
                "type": "CHART", "id": chart_layout_id, "parents": [row_1_id],
                "meta": {"chartId": chart_ids[1], "width": 6, "height": 50}
            }
            
        if len(chart_ids) >= 3: # Gráfico 3 (Hospitalizaciones)
            chart_layout_id = f"CHART-{chart_ids[2]}"
            position_json[row_2_id]["children"].append(chart_layout_id)
            position_json[chart_layout_id] = {
                "type": "CHART", "id": chart_layout_id, "parents": [row_2_id],
                "meta": {"chartId": chart_ids[2], "width": 6, "height": 50}
            }
            
        if len(chart_ids) >= 4: # Gráfico 4 (Geográfico)
            chart_layout_id = f"CHART-{chart_ids[3]}"
            position_json[row_2_id]["children"].append(chart_layout_id)
            position_json[chart_layout_id] = {
                "type": "CHART", "id": chart_layout_id, "parents": [row_2_id],
                "meta": {"chartId": chart_ids[3], "width": 6, "height": 50}
            }
        
        # 4. El payload para el PUT 
        dashboard_update_data = {
            "dashboard_title": dashboard_title,
            "published": True,
            "position_json": json.dumps(position_json)
        }
        
        try:
            data = json.dumps(dashboard_update_data).encode('utf-8')
            req = urllib.request.Request(f"{base_url}/api/v1/dashboard/{dashboard_id}", data=data, headers=auth_headers)
            req.get_method = lambda: 'PUT' 
            
            with urllib.request.urlopen(req, timeout=30) as response:
                if response.getcode() == 200: 
                    print(f"   ✅ Layout aplicado exitosamente!")
                    print(f"🔗 URL: {base_url}/superset/dashboard/{dashboard_id}/")
                else:
                    print(f"   ❌ Error HTTP {response.getcode()} aplicando layout")
                    
        except urllib.error.HTTPError as e:
            error_message = e.read().decode('utf-8')
            print(f"   ❌ Error HTTPError (Paso 3): {e.code} - {error_message}")
        except Exception as e:
            print(f"   ❌ Error (Paso 3): {str(e)}")

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