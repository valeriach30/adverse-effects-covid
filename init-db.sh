#!/bin/bash
set -e

echo "Creando bases de datos y usuarios..."

# Crear bases de datos
createdb -U postgres druid 2>/dev/null || echo "Base de datos 'druid' ya existe"
createdb -U postgres airflow 2>/dev/null || echo "Base de datos 'airflow' ya existe" 
createdb -U postgres superset 2>/dev/null || echo "Base de datos 'superset' ya existe"

# Crear usuarios
psql -U postgres -c "CREATE USER druid WITH PASSWORD 'FoolishPassword';" 2>/dev/null || echo "Usuario 'druid' ya existe"
psql -U postgres -c "CREATE USER airflow WITH PASSWORD 'airflow';" 2>/dev/null || echo "Usuario 'airflow' ya existe"
psql -U postgres -c "CREATE USER superset WITH PASSWORD 'superset';" 2>/dev/null || echo "Usuario 'superset' ya existe"

# Otorgar permisos a las bases de datos
psql -U postgres -c "GRANT ALL PRIVILEGES ON DATABASE druid TO druid;"
psql -U postgres -c "GRANT ALL PRIVILEGES ON DATABASE airflow TO airflow;"
psql -U postgres -c "GRANT ALL PRIVILEGES ON DATABASE superset TO superset;"

# Otorgar permisos específicos en esquemas públicos
psql -U postgres -d druid -c "GRANT ALL ON SCHEMA public TO druid;"
psql -U postgres -d druid -c "GRANT CREATE ON SCHEMA public TO druid;"
psql -U postgres -d airflow -c "GRANT ALL ON SCHEMA public TO airflow;"
psql -U postgres -d airflow -c "GRANT CREATE ON SCHEMA public TO airflow;"
psql -U postgres -d superset -c "GRANT ALL ON SCHEMA public TO superset;"
psql -U postgres -d superset -c "GRANT CREATE ON SCHEMA public TO superset;"

# Otorgar permisos para crear tablas en el futuro
psql -U postgres -d druid -c "ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO druid;"
psql -U postgres -d airflow -c "ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO airflow;"
psql -U postgres -d superset -c "ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO superset;"

echo "Configuración de base de datos completada."