#!/usr/bin/env python3
"""
Script para CREAR datasets en Superset desde tablas PostgreSQL
Si el dataset ya existe, se actualiza
"""

import json
import urllib.request
import urllib.parse
from http.cookiejar import CookieJar
import time

def create_superset_datasets():
    """Crear datasets VAERS en Superset desde PostgreSQL"""

    base_url = "http://superset:8088"
    print("📊 CREANDO DATASETS VAERS EN SUPERSET")
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

    try:
        with opener.open(req) as response:
            result = json.loads(response.read().decode('utf-8'))
            access_token = result['access_token']
            print("✅ Autenticación exitosa!")
    except Exception as e:
        print(f"❌ Error en autenticación: {e}")
        return False

    # Headers con token
    headers = {
        'Authorization': f'Bearer {access_token}',
        'Content-Type': 'application/json'
    }

    # Obtener CSRF token
    print("🔑 Obteniendo token CSRF...")
    try:
        req = urllib.request.Request(f"{base_url}/api/v1/security/csrf_token/", headers=headers)
        with opener.open(req) as response:
            csrf_result = json.loads(response.read().decode('utf-8'))
            csrf_token = csrf_result['result']
        headers['X-CSRFToken'] = csrf_token
        print("✅ Token CSRF obtenido!")
    except Exception as e:
        print(f"❌ Error obteniendo CSRF token: {e}")
        return False

    # Primero obtener ID de la conexión PostgreSQL
    print("\n🔍 Buscando conexión PostgreSQL...")
    try:
        req = urllib.request.Request(f"{base_url}/api/v1/database/", headers=headers)
        with opener.open(req) as response:
            databases = json.loads(response.read().decode('utf-8')).get('result', [])
        
        postgres_id = None
        for db in databases:
            if 'postgres' in db.get('database_name', '').lower() or 'superset' in db.get('database_name', '').lower():
                postgres_id = db['id']
                print(f"   ✅ Conexión PostgreSQL encontrada (ID: {postgres_id})")
                break
        
        if not postgres_id:
            print(f"   ❌ No se encontró conexión PostgreSQL. Bases disponibles: {[db.get('database_name') for db in databases]}")
            return False
    except Exception as e:
        print(f"❌ Error buscando base de datos: {e}")
        return False

    # Definir datasets a crear
    datasets_to_create = [
        {
            "table_name": "vaers_symptoms_analysis",
            "description": "Análisis de síntomas reportados por fabricante de vacuna"
        },
        {
            "table_name": "vaers_severity_analysis",
            "description": "Análisis de severidad (hospitalizaciones, muertes) por edad y fabricante"
        },
        {
            "table_name": "vaers_geographic_analysis",
            "description": "Distribución geográfica de reportes de efectos adversos"
        }
    ]

    created_datasets = {}

    # Crear cada dataset
    for dataset_config in datasets_to_create:
        table_name = dataset_config["table_name"]
        print(f"\n📝 Creando dataset para tabla: {table_name}")

        dataset_payload = {
            "database_id": postgres_id,
            "table_name": table_name,
            "schema": "public"
        }

        try:
            data = json.dumps(dataset_payload).encode('utf-8')
            req = urllib.request.Request(f"{base_url}/api/v1/dataset/",
                                        data=data,
                                        headers=headers)
            req.get_method = lambda: 'POST'

            with opener.open(req) as response:
                if response.getcode() in [201, 200]:
                    result = json.loads(response.read().decode('utf-8'))
                    dataset_id = result.get('id')
                    created_datasets[table_name] = dataset_id
                    print(f"   ✅ Dataset creado (ID: {dataset_id})")
                    
                    # Esperar un poco entre creaciones
                    time.sleep(1)
                else:
                    print(f"   ⚠️ Respuesta HTTP {response.getcode()}")

        except urllib.error.HTTPError as e:
            if e.code == 422:
                # Dataset ya existe, intentar actualizar
                print(f"   ℹ️ Dataset ya existe, buscando ID...")
                try:
                    search_url = f"{base_url}/api/v1/dataset/?q=(filters:!((col:table_name,opr:eq,value:{table_name})))"
                    req = urllib.request.Request(search_url, headers=headers)
                    with opener.open(req) as response:
                        search_result = json.loads(response.read().decode('utf-8'))
                        datasets = search_result.get('result', [])
                        if datasets:
                            dataset_id = datasets[0]['id']
                            created_datasets[table_name] = dataset_id
                            print(f"   ✅ Dataset existente encontrado (ID: {dataset_id})")
                except Exception as search_error:
                    print(f"   ❌ Error buscando dataset: {search_error}")
            else:
                print(f"   ❌ Error HTTP {e.code}: {e.read().decode('utf-8')}")

        except Exception as e:
            print(f"   ❌ Error creando dataset: {e}")

    if len(created_datasets) >= 3:
        print(f"\n✅ Se crearon/encontraron {len(created_datasets)} datasets VAERS")
        print("="*50)
        return True
    else:
        print(f"\n❌ Solo se crearon {len(created_datasets)} de 3 datasets requeridos")
        return False

if __name__ == "__main__":
    success = create_superset_datasets()
    exit(0 if success else 1)
