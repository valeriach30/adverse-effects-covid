#!/usr/bin/env python3
"""
Script para refrescar datasets en Superset y sincronizar columnas con Druid
"""

import json
import urllib.request
import urllib.parse
from http.cookiejar import CookieJar
import time

def refresh_superset_datasets():
    """Refrescar todos los datasets VAERS en Superset"""

    base_url = "http://superset:8088"
    print("🔄 REFRESCANDO DATASETS VAERS EN SUPERSET")
    print("="*50)

    # Setup cookies y autenticación
    cookie_jar = CookieJar()
    opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(cookie_jar))

    # Login
    print("🔐 Autenticando...")
    login_data = json.dumps({
        'username': 'admin',
        'password': 'admin',
        'provider': 'db'
    }).encode('utf-8')

    req = urllib.request.Request(f"{base_url}/api/v1/security/login",
                                data=login_data,
                                headers={'Content-Type': 'application/json'})

    with opener.open(req) as response:
        result = json.loads(response.read().decode('utf-8'))
        access_token = result['access_token']

    print("✅ Autenticación exitosa!")

    # Headers con token
    headers = {
        'Authorization': f'Bearer {access_token}',
        'Content-Type': 'application/json'
    }

    # Obtener CSRF token
    print("🔑 Obteniendo token CSRF...")
    req = urllib.request.Request(f"{base_url}/api/v1/security/csrf_token/", headers=headers)
    with opener.open(req) as response:
        csrf_result = json.loads(response.read().decode('utf-8'))
        csrf_token = csrf_result['result']

    headers['X-CSRFToken'] = csrf_token
    print("✅ Token CSRF obtenido!")

    # Buscar datasets VAERS
    print("🔍 Buscando datasets VAERS...")
    search_url = f"{base_url}/api/v1/dataset/?q=(filters:!((col:table_name,opr:sw,value:vaers)))"
    req = urllib.request.Request(search_url, headers=headers)

    with opener.open(req) as response:
        datasets_response = json.loads(response.read().decode('utf-8'))
        datasets = datasets_response['result']

    print(f"📋 Encontrados {len(datasets)} datasets VAERS")

    # Refrescar cada datase
    for dataset in datasets:
        dataset_id = dataset['id']
        table_name = dataset['table_name']

        print(f"\n🔄 Refrescando dataset: {table_name} (ID: {dataset_id})")

        try:
            # Llamar al endpoint de refresh
            refresh_url = f"{base_url}/api/v1/dataset/{dataset_id}/refresh"
            req = urllib.request.Request(refresh_url, headers=headers)
            req.get_method = lambda: 'PUT'

            with opener.open(req) as response:
                if response.getcode() == 200:
                    print(f"   ✅ {table_name} refrescado exitosamente")
                else:
                    print(f"   ⚠️ {table_name} - Código: {response.getcode()}")

            # Esperar un poco entre refreshes
            time.sleep(1)

        except Exception as e:
            print(f"   ❌ Error refrescando {table_name}: {str(e)}")

    print("\n🎉 REFRESH COMPLETADO!")
    print("="*50)

if __name__ == "__main__":
    refresh_superset_datasets()