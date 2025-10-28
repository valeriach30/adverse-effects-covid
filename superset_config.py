# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License a
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# Configuración de Superset para el proyecto VAERS
import os

# Base de datos para metadatos de Superset (PostgreSQL)
SQLALCHEMY_DATABASE_URI = 'postgresql://superset:superset@postgres:5432/superset'

# Configuración de Redis (opcional, para caché)
# REDIS_HOST = "redis"
# REDIS_PORT = 6379

# Configuración de seguridad
SECRET_KEY = 'unaclavesecretaparaSuperset'

# Configuración de conexiones por defecto a Druid
DRUID_BROKER_HOST = 'broker'
DRUID_BROKER_PORT = 8082

# Configuraciones adicionales para el proyecto VAERS
class Config:
    # Habilitamos la funcionalidad de SQL Lab para consultas ad-hoc
    SQLLAB_ASYNC_TIME_LIMIT_SEC = 300
    SQLLAB_TIMEOUT = 300

    # Configuración para trabajar con Druid
    ENABLE_PROXY_FIX = True

    # Configuración de logging
    LOG_LEVEL = 'INFO'

    # Configuración de funciones permitidas en SQL Lab
    PREVENT_UNSAFE_DB_CONNECTIONS = False

# Aplicar configuración
globals().update(Config.__dict__)

# Configuración de base de datos por defecto (metadatos)
SQLALCHEMY_DATABASE_URI = 'postgresql://superset:superset@postgres:5432/superset'

# Feature flags para habilitar funcionalidades específicas
FEATURE_FLAGS = {
    "DASHBOARD_NATIVE_FILTERS": True,
    "ENABLE_TEMPLATE_PROCESSING": True,
    "DASHBOARD_CROSS_FILTERS": True,
    "DASHBOARD_RBAC": True,
}

# Configuración de email (opcional)
# SMTP_HOST = "localhost"
# SMTP_STARTTLS = True
# SMTP_SSL = False
# SMTP_USER = "superset"
# SMTP_PORT = 25
# SMTP_PASSWORD = "superset"
# SMTP_MAIL_FROM = "superset@superset.com"

# Configuración de autenticación
AUTH_TYPE = 1  # Database authentication
AUTH_USER_REGISTRATION = True
AUTH_USER_REGISTRATION_ROLE = "Gamma"  # Rol por defecto para nuevos usuarios

# Configuración de cache
CACHE_CONFIG = {
    'CACHE_TYPE': 'filesystem',
    'CACHE_DIR': '/tmp/superset_cache',
    'CACHE_DEFAULT_TIMEOUT': 86400,  # 1 día
}

# Configuración para trabajar mejor con Druid
DRUID_IS_ACTIVE = True

print("Superset configurado para proyecto VAERS con conexión a Druid y PostgreSQL")