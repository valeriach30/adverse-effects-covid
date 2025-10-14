#!/usr/bin/env python3
"""
Script para verificar el estado de los servicios antes de configurar Superset
"""

import requests
import time
import json

def check_service_status():
    """Verificar estado de todos los servicios necesarios"""
    services = {
        "Druid Broker": "http://localhost:8082/status",
        "Druid Coordinator": "http://localhost:8081/status", 
        "Druid Router": "http://localhost:8888/status",
        "Superset": "http://localhost:8088/health",
        "PostgreSQL": None  # Verificamos indirectamente con Superset
    }
    
    print("🔍 Verificando estado de servicios...")
    print("="*50)
    
    all_ok = True
    
    for service_name, url in services.items():
        if url is None:
            continue
            
        try:
            response = requests.get(url, timeout=5)
            if response.status_code == 200:
                print(f"✅ {service_name}: FUNCIONANDO")
            else:
                print(f"⚠️ {service_name}: Responde con código {response.status_code}")
                all_ok = False
        except requests.exceptions.RequestException as e:
            print(f"❌ {service_name}: NO DISPONIBLE - {str(e)}")
            all_ok = False
    
    return all_ok

def check_druid_datasources():
    """Verificar qué datasources están disponibles en Druid"""
    print("\n📊 Verificando datasources en Druid...")
    
    try:
        # Intentar obtener lista de datasources del coordinator
        response = requests.get("http://localhost:8081/druid/coordinator/v1/datasources", timeout=10)
        
        if response.status_code == 200:
            datasources = response.json()
            if datasources:
                print(f"✅ Datasources encontrados: {len(datasources)}")
                for ds in datasources:
                    print(f"   - {ds}")
                return True
            else:
                print("⚠️ No hay datasources disponibles en Druid")
                return False
        else:
            print(f"❌ Error consultando datasources: {response.status_code}")
            return False
            
    except Exception as e:
        print(f"❌ Error verificando datasources: {str(e)}")
        return False

def test_druid_sql_direct():
    """Probar consulta SQL directa a Druid"""
    print("\n🧪 Probando consulta SQL directa a Druid...")
    
    try:
        # Consulta para listar tablas
        query = {"query": "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES LIMIT 10"}
        
        response = requests.post(
            "http://localhost:8082/druid/v2/sql",
            json=query,
            headers={"Content-Type": "application/json"},
            timeout=15
        )
        
        if response.status_code == 200:
            result = response.json()
            print("✅ Consulta SQL exitosa!")
            if result:
                print(f"   Tablas disponibles: {len(result)}")
                for row in result[:5]:  # Mostrar solo las primeras 5
                    print(f"   - {row.get('TABLE_NAME', 'N/A')}")
            return True
        else:
            print(f"❌ Error en consulta SQL: {response.status_code}")
            print(f"   Respuesta: {response.text}")
            return False
            
    except Exception as e:
        print(f"❌ Error en consulta SQL directa: {str(e)}")
        return False

def main():
    print("🔍 Verificación completa de servicios VAERS")
    print("="*50)
    
    # 1. Verificar servicios básicos
    services_ok = check_service_status()
    
    # 2. Verificar datasources de Druid
    datasources_ok = check_druid_datasources()
    
    # 3. Probar consulta SQL directa
    sql_ok = test_druid_sql_direct()
    
    # Resumen
    print("\n" + "="*50)
    print("📋 RESUMEN DE VERIFICACIÓN:")
    print(f"   Servicios básicos: {'✅ OK' if services_ok else '❌ FALLOS'}")
    print(f"   Datasources Druid: {'✅ OK' if datasources_ok else '❌ FALLOS'}")
    print(f"   Consultas SQL: {'✅ OK' if sql_ok else '❌ FALLOS'}")
    
    if services_ok and datasources_ok and sql_ok:
        print("\n🎉 ¡Todos los servicios están funcionando correctamente!")
        print("✨ Puedes proceder con la configuración de Superset")
        return True
    else:
        print("\n⚠️ Algunos servicios tienen problemas")
        print("💡 Asegúrate de que todos los contenedores estén ejecutándose:")
        print("   docker-compose ps")
        return False

if __name__ == "__main__":
    main()