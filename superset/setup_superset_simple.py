#!/usr/bin/env python3
"""
Script simple para configurar Superset con Druid - Configuración básica
Objetivo: Verificar conectividad y crear una conexión funcional
"""

import requests
import json
import time
import sys
from urllib.parse import urljoin

class SupersetSimpleSetup:
    def __init__(self):
        self.base_url = "http://localhost:8088"
        self.session = requests.Session()
        self.access_token = None
        self.csrf_token = None
        
    def wait_for_superset(self, max_attempts=20):
        """Esperar a que Superset esté disponible"""
        print("⏳ Esperando a que Superset esté disponible...")
        
        for attempt in range(max_attempts):
            try:
                response = requests.get(f"{self.base_url}/health", timeout=5)
                if response.status_code == 200:
                    print("✅ Superset está disponible!")
                    return True
            except requests.exceptions.RequestException:
                pass
            
            print(f"   Intento {attempt + 1}/{max_attempts}... esperando 10 segundos")
            time.sleep(10)
        
        print("❌ Superset no está disponible después de esperar")
        return False
    
    def get_csrf_token(self):
        """Obtener token CSRF necesario para las requests"""
        try:
            response = self.session.get(f"{self.base_url}/api/v1/security/csrf_token/")
            if response.status_code == 200:
                result = response.json()
                self.csrf_token = result.get("result")
                self.session.headers.update({
                    "X-CSRFToken": self.csrf_token
                })
                print("✅ Token CSRF obtenido")
                return True
            else:
                print(f"❌ Error obteniendo CSRF token: {response.status_code}")
                return False
        except Exception as e:
            print(f"❌ Error obteniendo CSRF: {str(e)}")
            return False

    def login(self):
        """Autenticación en Superset"""
        print("🔐 Iniciando sesión en Superset...")
        
        # Datos de login
        login_data = {
            "username": "admin",
            "password": "admin",
            "provider": "db",
            "refresh": True
        }
        
        try:
            response = self.session.post(
                f"{self.base_url}/api/v1/security/login",
                json=login_data,
                timeout=30
            )
            
            if response.status_code == 200:
                result = response.json()
                self.access_token = result.get("access_token")
                
                # Configurar headers para futuras requests
                self.session.headers.update({
                    "Authorization": f"Bearer {self.access_token}",
                    "Content-Type": "application/json"
                })
                
                print("✅ Autenticación exitosa en Superset")
                
                # Obtener CSRF token después del login
                if self.get_csrf_token():
                    return True
                else:
                    print("❌ No se pudo obtener token CSRF")
                    return False
            else:
                print(f"❌ Error de autenticación: {response.status_code} - {response.text}")
                return False
                
        except Exception as e:
            print(f"❌ Error durante login: {str(e)}")
            return False
    
    def test_druid_connection(self):
        """Probar conexión a Druid desde fuera del contenedor"""
        print("🔍 Probando conectividad con Druid...")
        
        try:
            # Probar desde el host
            druid_url = "http://localhost:8082/status"
            response = requests.get(druid_url, timeout=10)
            
            if response.status_code == 200:
                print("✅ Druid broker accesible desde el host")
                return True
            else:
                print(f"⚠️ Druid responde con código: {response.status_code}")
                return False
                
        except Exception as e:
            print(f"❌ Error conectando a Druid: {str(e)}")
            return False
    
    def create_druid_database(self):
        """Crear conexión de base de datos a Druid"""
        print("📊 Creando conexión con Druid...")
        
        # Configuración de la base de datos Druid
        database_config = {
            "database_name": "vaers_druid_simple",
            "sqlalchemy_uri": "druid://broker:8082/druid/v2/sql/",
            "expose_in_sqllab": True,
            "allow_ctas": False,
            "allow_cvas": False,
            "allow_dml": False,
            "cache_timeout": 0,
            "extra": json.dumps({
                "metadata_params": {},
                "engine_params": {
                    "connect_args": {}
                }
            })
        }
        
        try:
            response = self.session.post(
                f"{self.base_url}/api/v1/database/",
                json=database_config,
                timeout=30
            )
            
            if response.status_code == 201:
                result = response.json()
                database_id = result.get("id")
                print(f"✅ Base de datos Druid creada exitosamente (ID: {database_id})")
                return database_id
            elif response.status_code == 422:
                print("⚠️ La base de datos ya existe o hay un error de validación")
                print(f"   Detalles: {response.text}")
                # Intentar obtener la base de datos existente
                return self.get_existing_database()
            else:
                print(f"❌ Error creando base de datos: {response.status_code}")
                print(f"   Respuesta: {response.text}")
                return None
                
        except Exception as e:
            print(f"❌ Excepción creando base de datos: {str(e)}")
            return None
    
    def get_existing_database(self):
        """Obtener ID de base de datos existente"""
        try:
            response = self.session.get(f"{self.base_url}/api/v1/database/")
            if response.status_code == 200:
                databases = response.json().get("result", [])
                for db in databases:
                    if "druid" in db.get("database_name", "").lower():
                        print(f"✅ Encontrada base de datos existente: {db['database_name']} (ID: {db['id']})")
                        return db["id"]
        except Exception as e:
            print(f"❌ Error buscando bases de datos existentes: {str(e)}")
        return None
    
    def test_druid_query(self, database_id):
        """Probar una consulta simple a Druid"""
        print("🧪 Probando consulta SQL a Druid...")
        
        # Consulta simple para listar datasources
        test_query = "SELECT datasource FROM INFORMATION_SCHEMA.SCHEMATA"
        
        query_data = {
            "database_id": database_id,
            "sql": test_query,
            "limit": 10
        }
        
        try:
            response = self.session.post(
                f"{self.base_url}/api/v1/sqllab/execute/",
                json=query_data,
                timeout=30
            )
            
            if response.status_code == 200:
                result = response.json()
                print("✅ Consulta ejecutada exitosamente!")
                print(f"   Resultado: {result}")
                return True
            else:
                print(f"❌ Error en consulta: {response.status_code}")
                print(f"   Respuesta: {response.text}")
                return False
                
        except Exception as e:
            print(f"❌ Excepción ejecutando consulta: {str(e)}")
            return False

def main():
    """Función principal - configuración básica"""
    print("🚀 Iniciando configuración básica de Superset + Druid")
    print("="*60)
    
    setup = SupersetSimpleSetup()
    
    # Paso 1: Esperar a que Superset esté disponible
    if not setup.wait_for_superset():
        sys.exit(1)
    
    # Paso 2: Autenticación
    if not setup.login():
        sys.exit(1)
    
    # Paso 3: Probar conexión con Druid
    if not setup.test_druid_connection():
        print("⚠️ Druid no está accesible, pero continuamos...")
    
    # Paso 4: Crear conexión de base de datos
    database_id = setup.create_druid_database()
    if not database_id:
        print("❌ No se pudo crear la conexión con Druid")
        sys.exit(1)
    
    # Paso 5: Probar consulta
    if setup.test_druid_query(database_id):
        print("\n✅ Configuración básica completada exitosamente!")
        print(f"🌐 Acceso a Superset: http://localhost:8088")
        print(f"🔑 Usuario: admin / Contraseña: admin")
        print(f"📊 Base de datos Druid creada con ID: {database_id}")
    else:
        print("\n⚠️ Configuración parcial - conexión creada pero consulta falló")
    
    print("="*60)

if __name__ == "__main__":
    main()