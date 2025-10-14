#!/usr/bin/env python3
"""
Script final para integrar con DAG de Airflow
Configuración automática de Superset usando solo urllib (sin requests)
"""

import urllib.request
import urllib.parse
import json
import time

def configure_superset_for_dag():
    """Función para usar en DAG de Airflow - solo con urllib"""
    print("🎯 Configurando Superset automáticamente desde DAG...")
    
    # Usar localhost cuando se ejecuta fuera del contenedor, superset cuando es desde DAG
    import os
    if os.path.exists('/opt/airflow'):
        base_url = "http://superset:8088"  # Desde contenedor de Airflow
    else:
        base_url = "http://localhost:8088"  # Desde host
    
    # Esperar disponibilidad
    print("⏳ Esperando disponibilidad de Superset...")
    for attempt in range(15):
        try:
            response = urllib.request.urlopen(f"{base_url}/health", timeout=5)
            if response.getcode() == 200:
                print("✅ Superset disponible!")
                break
        except Exception:
            pass
        
        if attempt == 14:
            raise Exception("Superset no disponible")
        
        print(f"   Intento {attempt + 1}/15...")
        time.sleep(10)
    
    # Autenticación
    print("🔐 Autenticando...")
    login_data = {
        "username": "admin",
        "password": "admin", 
        "provider": "db",
        "refresh": True
    }
    
    data = json.dumps(login_data).encode('utf-8')
    req = urllib.request.Request(
        f"{base_url}/api/v1/security/login",
        data=data,
        headers={'Content-Type': 'application/json'}
    )
    
    with urllib.request.urlopen(req, timeout=30) as response:
        result = json.loads(response.read().decode('utf-8'))
        access_token = result.get("access_token")
    
    if not access_token:
        raise Exception("No se pudo obtener token de acceso")
    
    # CSRF Token
    req = urllib.request.Request(
        f"{base_url}/api/v1/security/csrf_token/",
        headers={'Authorization': f'Bearer {access_token}'}
    )
    
    with urllib.request.urlopen(req, timeout=30) as response:
        csrf_result = json.loads(response.read().decode('utf-8'))
        csrf_token = csrf_result.get("result")
    
    headers = {
        'Authorization': f'Bearer {access_token}',
        'Content-Type': 'application/json',
        'X-CSRFToken': csrf_token
    }
    
    print("✅ Autenticación exitosa")
    
    # Configurar base de datos Druid
    print("📊 Configurando conexión Druid...")
    
    # Verificar si existe
    req = urllib.request.Request(f"{base_url}/api/v1/database/", headers=headers)
    with urllib.request.urlopen(req, timeout=30) as response:
        databases = json.loads(response.read().decode('utf-8')).get("result", [])
    
    database_id = None
    for db in databases:
        if "druid" in db.get("database_name", "").lower():
            database_id = db["id"]
            print(f"✅ Usando BD existente: {db['database_name']}")
            break
    
    if not database_id:
        # Crear nueva conexión
        database_config = {
            "database_name": "VAERS_Druid_DAG",
            "sqlalchemy_uri": "druid://broker:8082/druid/v2/sql/",
            "expose_in_sqllab": True
        }
        
        data = json.dumps(database_config).encode('utf-8')
        req = urllib.request.Request(f"{base_url}/api/v1/database/", data=data, headers=headers)
        
        with urllib.request.urlopen(req, timeout=30) as response:
            result = json.loads(response.read().decode('utf-8'))
            database_id = result.get("id")
        
        print(f"✅ Nueva BD creada (ID: {database_id})")
    
    # Crear datasets principales
    print("📈 Creando datasets...")
    
    tables = ["vaers_symptoms_by_manufacturer", "vaers_severity_by_age", "vaers_geographic_distribution"]
    datasets_created = 0
    
    for table in tables:
        dataset_config = {
            "database": database_id,
            "table_name": table
        }
        
        try:
            data = json.dumps(dataset_config).encode('utf-8')
            req = urllib.request.Request(f"{base_url}/api/v1/dataset/", data=data, headers=headers)
            
            with urllib.request.urlopen(req, timeout=30) as response:
                if response.getcode() == 201:
                    datasets_created += 1
                    print(f"   ✅ Dataset: {table}")
        except Exception:
            # Probablemente ya existe
            print(f"   ℹ️ Dataset ya existe: {table}")
            datasets_created += 1
    
    # Resultado final
    if database_id and datasets_created > 0:
        print(f"\n🎉 Configuración automática completada!")
        print(f"📊 Base de datos ID: {database_id}")
        print(f"📈 Datasets: {datasets_created}")
        print(f"🌐 Acceso: http://localhost:8088")
        print(f"🔑 Credenciales: admin / admin")
        
        return f"Superset configurado: BD {database_id}, {datasets_created} datasets"
    else:
        raise Exception("Error en configuración de Superset")

# Para usar en el DAG
if __name__ == "__main__":
    try:
        result = configure_superset_for_dag()
        print(result)
    except Exception as e:
        print(f"❌ Error: {str(e)}")
        exit(1)