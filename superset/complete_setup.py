#!/usr/bin/env python3
"""
Script unificado para configurar COMPLETAMENTE Superset:
1. Crear conexión DB PostgreSQL si no existe
2. Crear datasets VAERS si no existen
3. Refrescar datasets para sincronizar columnas
4. Crear dashboard con gráficos

TODO EN UN SOLO PASO
"""

import json
import time
import urllib.request
import urllib.parse
import urllib.error
from http.cookiejar import CookieJar

def setup_superset_complete():
    """Setup completo de Superset: DB + Datasets + Refresh + Dashboard"""

    base_url = "http://superset:8088"
    print("🚀 CONFIGURACIÓN COMPLETA DE SUPERSET")
    print("="*60)

    # ===== 1. VERIFICAR DISPONIBILIDAD =====
    print("\n⏳ [1/4] Verificando Superset...")
    for i in range(10):
        try:
            req = urllib.request.Request(f"{base_url}/health")
            with urllib.request.urlopen(req, timeout=5) as response:
                if response.getcode() == 200:
                    print("✅ Superset disponible!")
                    break
        except Exception:
            print(f"   Intento {i+1}/10...")
            time.sleep(3)
    else:
        print("❌ Superset no disponible")
        return False

    # ===== 2. AUTENTICACIÓN =====
    print("\n🔐 [2/4] Autenticando...")
    cookie_jar = CookieJar()
    opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(cookie_jar))

    login_data = json.dumps({
        "username": "admin",
        "password": "admin",
        "refresh": True,
        "provider": "db"
    }).encode('utf-8')

    try:
        req = urllib.request.Request(f"{base_url}/api/v1/security/login",
                                    data=login_data,
                                    headers={'Content-Type': 'application/json'})
        with opener.open(req, timeout=30) as response:
            result = json.loads(response.read().decode('utf-8'))
            access_token = result.get("access_token")
        print("✅ Autenticación exitosa!")
    except Exception as e:
        print(f"❌ Error en autenticación: {e}")
        return False

    headers = {
        'Authorization': f'Bearer {access_token}',
        'Content-Type': 'application/json',
        'Accept': 'application/json'
    }

    # Obtener CSRF token
    try:
        req = urllib.request.Request(f"{base_url}/api/v1/security/csrf_token/", headers=headers)
        with opener.open(req, timeout=30) as response:
            csrf_token = json.loads(response.read().decode('utf-8')).get("result")
        headers['X-CSRFToken'] = csrf_token
    except Exception as e:
        print(f"❌ Error obteniendo CSRF: {e}")
        return False

    # ===== 3. CONFIGURAR BASE DE DATOS =====
    print("\n🔌 [3/4] Configurando conexión PostgreSQL...")
    try:
        req = urllib.request.Request(f"{base_url}/api/v1/database/", headers=headers)
        with opener.open(req, timeout=30) as response:
            databases = json.loads(response.read().decode('utf-8')).get('result', [])
        
        postgres_id = None
        for db in databases:
            db_name = db.get('database_name', '').lower()
            if 'superset' in db_name or 'postgresql' in db_name:
                postgres_id = db['id']
                print(f"   ✅ Conexión PostgreSQL ya existe (ID: {postgres_id})")
                break
        
        # Crear si no existe
        if not postgres_id:
            print("   ℹ️ Creando conexión PostgreSQL...")
            db_payload = {
                "database_name": "PostgreSQL Superset",
                "sqlalchemy_uri": "postgresql://superset:superset@postgres:5432/superset",
                "expose_in_sqllab": True,
                "allow_csv_upload": True
            }
            data = json.dumps(db_payload).encode('utf-8')
            req = urllib.request.Request(f"{base_url}/api/v1/database/", data=data, headers=headers)
            req.get_method = lambda: 'POST'
            with opener.open(req, timeout=30) as response:
                postgres_id = json.loads(response.read().decode('utf-8')).get('id')
            print(f"   ✅ Conexión creada (ID: {postgres_id})")
    except Exception as e:
        print(f"❌ Error configurando DB: {e}")
        return False

    # ===== 4. CREAR/VERIFICAR DATASETS =====
    print("\n📊 [4/4] Configurando datasets...")
    dataset_ids = {}
    tables = ["vaers_symptoms_analysis", "vaers_severity_analysis", "vaers_geographic_analysis"]
    
    for table in tables:
        try:
            # Buscar si ya existe
            search_url = f"{base_url}/api/v1/dataset/?q=(filters:!((col:table_name,opr:eq,value:{table})))"
            req = urllib.request.Request(search_url, headers=headers)
            with opener.open(req, timeout=30) as response:
                results = json.loads(response.read().decode('utf-8')).get('result', [])
            
            if results:
                dataset_ids[table] = results[0]['id']
                print(f"   ✅ {table} ya existe (ID: {dataset_ids[table]})")
                
                # Refrescar dataset
                refresh_url = f"{base_url}/api/v1/dataset/{dataset_ids[table]}/refresh"
                req = urllib.request.Request(refresh_url, headers=headers)
                req.get_method = lambda: 'PUT'
                opener.open(req, timeout=30)
                print(f"      ↻ Refrescado")
            else:
                # Crear dataset
                print(f"   ℹ️ Creando {table}...")
                ds_payload = {
                    "database": postgres_id,
                    "table_name": table,
                    "schema": "public"
                }
                data = json.dumps(ds_payload).encode('utf-8')
                req = urllib.request.Request(f"{base_url}/api/v1/dataset/", data=data, headers=headers)
                req.get_method = lambda: 'POST'
                with opener.open(req, timeout=30) as response:
                    dataset_ids[table] = json.loads(response.read().decode('utf-8')).get('id')
                print(f"   ✅ {table} creado (ID: {dataset_ids[table]})")
        except Exception as e:
            print(f"   ⚠️ Error con {table}: {str(e)[:100]}")

    if len(dataset_ids) < 3:
        print(f"\n❌ Solo se configuraron {len(dataset_ids)} de 3 datasets")
        return False

    # ===== 5. CREAR DASHBOARD =====
    print("\n🎨 Creando dashboard...")
    
    # Crear dashboard vacío
    dashboard_data = {
        "dashboard_title": "📊 VAERS COVID-19 - Dashboard Completo",
        "published": True
    }
    try:
        data = json.dumps(dashboard_data).encode('utf-8')
        req = urllib.request.Request(f"{base_url}/api/v1/dashboard/", data=data, headers=headers)
        req.get_method = lambda: 'POST'
        with opener.open(req, timeout=30) as response:
            dashboard_id = json.loads(response.read().decode('utf-8')).get("id")
        print(f"   ✅ Dashboard creado (ID: {dashboard_id})")
    except Exception as e:
        print(f"   ⚠️ Error creando dashboard: {e}")
        return False

    # Crear gráficos
    chart_ids = []
    charts_config = [
        {
            "name": "📊 Reportes por Fabricante",
            "dataset": "vaers_symptoms_analysis",
            "viz_type": "pie",
            "params": {
                "groupby": ["manufacturer"],
                "metric": {
                    "aggregate": "SUM",
                    "column": {"column_name": "total_reports", "type": "BIGINT"},
                    "expressionType": "SIMPLE",
                    "label": "Total Reportes"
                }
            }
        },
        {
            "name": "📈 Top 15 Síntomas",
            "dataset": "vaers_symptoms_analysis",
            "viz_type": "dist_bar",
            "params": {
                "groupby": ["symptom"],
                "metrics": [{
                    "aggregate": "SUM",
                    "column": {"column_name": "total_reports"},
                    "expressionType": "SIMPLE",
                    "label": "Total Reportes"
                }],
                "row_limit": 15,
                "order_desc": True
            }
        },
        {
            "name": "🏥 Hospitalizaciones por Edad",
            "dataset": "vaers_severity_analysis",
            "viz_type": "dist_bar",
            "params": {
                "groupby": ["age_group"],
                "metrics": [{
                    "aggregate": "SUM",
                    "column": {"column_name": "hospitalizations"},
                    "expressionType": "SIMPLE",
                    "label": "Hospitalizaciones"
                }],
                "order_desc": True
            }
        },
        {
            "name": "🗺️ Distribución Geográfica",
            "dataset": "vaers_geographic_analysis",
            "viz_type": "dist_bar",
            "params": {
                "groupby": ["state"],
                "metrics": [{
                    "aggregate": "SUM",
                    "column": {"column_name": "total_reports"},
                    "expressionType": "SIMPLE",
                    "label": "Total Reportes"
                }],
                "row_limit": 15,
                "order_desc": True
            }
        }
    ]

    for chart_cfg in charts_config:
        try:
            chart_data = {
                "slice_name": chart_cfg["name"],
                "datasource_id": dataset_ids[chart_cfg["dataset"]],
                "datasource_type": "table",
                "viz_type": chart_cfg["viz_type"],
                "dashboards": [dashboard_id],
                "params": json.dumps(chart_cfg["params"])
            }
            data = json.dumps(chart_data).encode('utf-8')
            req = urllib.request.Request(f"{base_url}/api/v1/chart/", data=data, headers=headers)
            req.get_method = lambda: 'POST'
            with opener.open(req, timeout=30) as response:
                chart_id = json.loads(response.read().decode('utf-8')).get("id")
                chart_ids.append(chart_id)
            print(f"   ✅ {chart_cfg['name']} creado")
        except Exception as e:
            print(f"   ⚠️ Error en {chart_cfg['name']}: {str(e)[:80]}")

    print(f"\n🎉 CONFIGURACIÓN COMPLETADA!")
    print(f"   📊 Dashboard: http://superset:8088/superset/dashboard/{dashboard_id}/")
    print(f"   📈 {len(chart_ids)} gráficos creados")
    print("="*60)
    return True

if __name__ == "__main__":
    success = setup_superset_complete()
    exit(0 if success else 1)
