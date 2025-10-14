#!/usr/bin/env python3
"""
Script de verificación final para comprobar que Superset tiene todo configurado
"""

import requests
import json

def check_superset_setup():
    """Verificar configuración completa de Superset"""
    print("🔍 Verificando configuración completa de Superset...")
    print("="*60)
    
    base_url = "http://localhost:8088"
    
    # Login
    session = requests.Session()
    login_data = {
        "username": "admin",
        "password": "admin",
        "provider": "db",
        "refresh": True
    }
    
    try:
        response = session.post(f"{base_url}/api/v1/security/login", json=login_data)
        if response.status_code != 200:
            print("❌ No se pudo autenticar")
            return False
        
        access_token = response.json().get("access_token")
        session.headers.update({
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json"
        })
        
        # CSRF Token
        csrf_response = session.get(f"{base_url}/api/v1/security/csrf_token/")
        csrf_token = csrf_response.json().get("result")
        session.headers.update({"X-CSRFToken": csrf_token})
        
        print("✅ Autenticación exitosa")
        
        # Verificar bases de datos
        db_response = session.get(f"{base_url}/api/v1/database/")
        databases = db_response.json().get("result", [])
        
        druid_dbs = [db for db in databases if "druid" in db.get("database_name", "").lower()]
        print(f"✅ Bases de datos Druid: {len(druid_dbs)}")
        for db in druid_dbs:
            print(f"   - {db['database_name']} (ID: {db['id']})")
        
        # Verificar datasets  
        dataset_response = session.get(f"{base_url}/api/v1/dataset/")
        datasets = dataset_response.json().get("result", [])
        
        vaers_datasets = [ds for ds in datasets if "vaers" in ds.get("table_name", "").lower()]
        print(f"✅ Datasets VAERS: {len(vaers_datasets)}")
        for ds in vaers_datasets:
            print(f"   - {ds['table_name']} (ID: {ds['id']})")
        
        # Verificar gráficos
        chart_response = session.get(f"{base_url}/api/v1/chart/")
        charts = chart_response.json().get("result", [])
        
        vaers_charts = [c for c in charts if "vaers" in c.get("slice_name", "").lower() or 
                       any(keyword in c.get("slice_name", "").lower() for keyword in 
                           ["reportes", "síntomas", "fabricante", "hospitalización"])]
        print(f"✅ Gráficos VAERS: {len(vaers_charts)}")
        for chart in vaers_charts:
            print(f"   - {chart['slice_name']} (ID: {chart['id']})")
        
        # Verificar dashboards
        dashboard_response = session.get(f"{base_url}/api/v1/dashboard/")
        dashboards = dashboard_response.json().get("result", [])
        
        vaers_dashboards = [d for d in dashboards if "vaers" in d.get("dashboard_title", "").lower() or
                           "análisis" in d.get("dashboard_title", "").lower()]
        print(f"✅ Dashboards VAERS: {len(vaers_dashboards)}")
        for dash in vaers_dashboards:
            print(f"   - {dash['dashboard_title']} (ID: {dash['id']})")
            print(f"     URL: {base_url}/dashboard/{dash['id']}")
        
        # Resumen
        print("\n" + "="*60)
        print("📋 RESUMEN DE CONFIGURACIÓN:")
        print(f"   Bases de datos Druid: {len(druid_dbs)} ✅")
        print(f"   Datasets VAERS: {len(vaers_datasets)} ✅") 
        print(f"   Gráficos VAERS: {len(vaers_charts)} ✅")
        print(f"   Dashboards VAERS: {len(vaers_dashboards)} ✅")
        
        if druid_dbs and vaers_datasets and vaers_charts:
            print("\n🎉 ¡Configuración de Superset completada exitosamente!")
            print(f"🌐 Acceso principal: {base_url}/dashboard/list/")
            print("🔑 Credenciales: admin / admin")
            
            if vaers_dashboards:
                print(f"📊 Dashboard principal: {base_url}/dashboard/{vaers_dashboards[0]['id']}")
            
            return True
        else:
            print("\n⚠️ Configuración incompleta")
            return False
            
    except Exception as e:
        print(f"❌ Error verificando configuración: {str(e)}")
        return False

if __name__ == "__main__":
    success = check_superset_setup()
    if not success:
        exit(1)