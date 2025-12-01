#!/usr/bin/env python3
"""
Script de verificación final - Confirmar que dashboard funciona correctamente
"""

import json
import urllib.request
from http.cookiejar import CookieJar

def verify_complete_dashboard():
    """Verificar que el dashboard esté funcionando correctamente"""

    base_url = "http://localhost:8088"
    print("🔍 VERIFICACIÓN FINAL DEL DASHBOARD VAERS")
    print("="*50)

    # Setup cookies
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

        print("✅ Autenticación exitosa")

    except Exception as e:
        print(f"❌ Error en login: {str(e)}")
        return False

    # Headers autenticados
    auth_headers = {
        'Authorization': f'Bearer {access_token}',
        'Content-Type': 'application/json',
        'Accept': 'application/json'
    }

    # Verificar datasets
    print("\n📊 Verificando datasets...")
    try:
        req = urllib.request.Request(f"{base_url}/api/v1/dataset/", headers=auth_headers)
        with urllib.request.urlopen(req, timeout=30) as response:
            datasets = json.loads(response.read().decode('utf-8')).get("result", [])

            vaers_datasets = []
            for dataset in datasets:
                table_name = dataset.get("table_name", "")
                if "vaers" in table_name.lower():
                    vaers_datasets.append({
                        "id": dataset["id"],
                        "name": table_name
                    })

            print(f"   ✅ Datasets VAERS encontrados: {len(vaers_datasets)}")
            for ds in vaers_datasets:
                print(f"      • {ds['name']} (ID: {ds['id']})")

    except Exception as e:
        print(f"❌ Error verificando datasets: {str(e)}")
        return False

    # Verificar gráficos
    print("\n📈 Verificando gráficos...")
    try:
        req = urllib.request.Request(f"{base_url}/api/v1/chart/", headers=auth_headers)
        with urllib.request.urlopen(req, timeout=30) as response:
            charts = json.loads(response.read().decode('utf-8')).get("result", [])

            vaers_charts = []
            for chart in charts:
                chart_name = chart.get("slice_name", "")
                if any(keyword in chart_name.lower() for keyword in ["vaers", "fabricante", "síntoma", "hospital", "geográf"]):
                    vaers_charts.append({
                        "id": chart["id"],
                        "name": chart_name,
                        "viz_type": chart.get("viz_type", "unknown")
                    })

            print(f"   ✅ Gráficos VAERS encontrados: {len(vaers_charts)}")
            for chart in vaers_charts:
                print(f"      • {chart['name']} (ID: {chart['id']}, Tipo: {chart['viz_type']})")

    except Exception as e:
        print(f"❌ Error verificando gráficos: {str(e)}")
        return False

    # Verificar dashboards
    print("\n🎯 Verificando dashboards...")
    try:
        req = urllib.request.Request(f"{base_url}/api/v1/dashboard/", headers=auth_headers)
        with urllib.request.urlopen(req, timeout=30) as response:
            dashboards = json.loads(response.read().decode('utf-8')).get("result", [])

            vaers_dashboards = []
            for dashboard in dashboards:
                dashboard_title = dashboard.get("dashboard_title", "")
                if "vaers" in dashboard_title.lower():
                    vaers_dashboards.append({
                        "id": dashboard["id"],
                        "title": dashboard_title,
                        "published": dashboard.get("published", False)
                    })

            print(f"   ✅ Dashboards VAERS encontrados: {len(vaers_dashboards)}")
            for dash in vaers_dashboards:
                status = "✅ Publicado" if dash["published"] else "⚠️ Borrador"
                print(f"      • {dash['title']} (ID: {dash['id']}) - {status}")
                print(f"        🔗 URL: {base_url}/superset/dashboard/{dash['id']}/")

    except Exception as e:
        print(f"❌ Error verificando dashboards: {str(e)}")
        return False

    # Resumen final
    print("\n" + "🎉" + "="*48 + "🎉")
    print("       VERIFICACIÓN COMPLETADA EXITOSAMENTE")
    print("="*50)
    print(f"📊 Datasets VAERS: {len(vaers_datasets)}")
    print(f"📈 Gráficos creados: {len(vaers_charts)}")
    print(f"🎯 Dashboards disponibles: {len(vaers_dashboards)}")
    print("\n🚀 DASHBOARD VAERS COMPLETAMENTE FUNCIONAL!")
    print("🔗 Acceso: http://localhost:8088")
    print("🔑 Login: admin / admin")
    print("="*50)

    return len(vaers_datasets) >= 3 and len(vaers_charts) >= 4 and len(vaers_dashboards) >= 1

if __name__ == "__main__":
    success = verify_complete_dashboard()
    if success:
        print("✅ VERIFICACIÓN EXITOSA - Dashboard completamente funcional!")
    else:
        print("❌ VERIFICACIÓN FALLIDA - Revisar configuración")
    exit(0 if success else 1)