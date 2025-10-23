#!/bin/bash

echo "🐻‍❄️ Iniciando Pipeline VAERS con Polars"
echo "========================================"
echo "📊 CSV → 🐻‍❄️ Polars ETL → 🐲 Druid + 🗄️ PostgreSQL → 📈 Superset"
echo ""

# Función para verificar si Docker está corriendo
check_docker() {
    if ! docker info > /dev/null 2>&1; then
        echo "❌ Docker no está corriendo. Por favor inicia Docker Desktop."
        exit 1
    fi
    echo "✅ Docker está corriendo"
}

# Función para verificar archivos de datos
check_data() {
    echo "🔍 Verificando archivos de datos..."
    
    if [ ! -d "data" ]; then
        echo "❌ Directorio 'data' no existe"
        echo "📥 Crea el directorio 'data' y coloca los archivos CSV de VAERS:"
        echo "   - VAERSDATA.csv"
        echo "   - VAERSSYMPTOMS.csv" 
        echo "   - VAERSVAX.csv"
        exit 1
    fi
    
    files=("VAERSDATA.csv" "VAERSSYMPTOMS.csv" "VAERSVAX.csv")
    all_exist=true
    
    for file in "${files[@]}"; do
        if [ -f "data/$file" ]; then
            size=$(du -h "data/$file" | cut -f1)
            echo "✅ $file ($size)"
        else
            echo "❌ data/$file no encontrado"
            all_exist=false
        fi
    done
    
    if [ "$all_exist" = false ]; then
        echo ""
        echo "📥 Descarga los archivos VAERS desde:"
        echo "   https://vaers.hhs.gov/data/datasets.html"
        echo "   Busca 'COVID19 Data' y descarga los archivos CSV más recientes"
        exit 1
    fi
}

# Función para limpiar contenedores anteriores
cleanup_old() {
    echo "🧹 Limpiando contenedores anteriores..."
    
    # Detener y remover contenedores relacionados
    docker compose -f docker-compose-polars.yml down 2>/dev/null || true
    docker compose down 2>/dev/null || true
    
    # Limpiar contenedores con nombres específicos
    containers=(
        "volume_init_vaers_polars"
        "postgres_polars"
        "airflow_init_polars" 
        "airflow_webserver_polars"
        "airflow_scheduler_polars"
        "zookeeper_polars"
        "coordinator_polars"
        "broker_polars"
        "historical_polars"
        "middlemanager_polars"
        "router_polars"
        "superset_polars"
        "monitor_polars"
    )
    
    for container in "${containers[@]}"; do
        if docker ps -a --format "table {{.Names}}" | grep -q "^$container$"; then
            echo "🗑️ Removiendo contenedor: $container"
            docker rm -f "$container" 2>/dev/null || true
        fi
    done
    
    echo "✅ Limpieza completada"
}

# Función para iniciar servicios
start_services() {
    echo "🚀 Iniciando servicios..."
    
    # Usar docker-compose-polars.yml si existe, sino docker-compose.yml
    if [ -f "docker-compose-polars.yml" ]; then
        compose_file="docker-compose-polars.yml"
        echo "📋 Usando: $compose_file"
    else
        compose_file="docker-compose.yml"
        echo "📋 Usando: $compose_file"
    fi
    
    # Iniciar servicios en orden
    echo "📦 Iniciando servicios base..."
    docker compose -f "$compose_file" up -d volume-init postgres zookeeper
    
    echo "⏳ Esperando a que PostgreSQL esté listo..."
    sleep 15
    
    echo "🌊 Iniciando Airflow..."
    docker compose -f "$compose_file" up -d airflow-init
    sleep 10
    docker compose -f "$compose_file" up -d airflow-webserver airflow-scheduler
    
    echo "🐲 Iniciando Druid..."
    docker compose -f "$compose_file" up -d coordinator broker historical middlemanager router
    
    echo "📈 Iniciando Superset..."
    docker compose -f "$compose_file" up -d superset
    
    echo "🔍 Iniciando monitor..."
    docker compose -f "$compose_file" up -d monitor
    
    echo "✅ Todos los servicios iniciados"
}

# Función para mostrar estado
show_status() {
    echo ""
    echo "📊 Estado de los servicios:"
    echo "=========================="
    
    services=(
        "postgres_polars:PostgreSQL:5432"
        "airflow_webserver_polars:Airflow:8080"
        "router_polars:Druid:8888"
        "superset_polars:Superset:8088"
    )
    
    for service_info in "${services[@]}"; do
        IFS=':' read -r container service port <<< "$service_info"
        
        if docker ps --format "table {{.Names}}\t{{.Status}}" | grep -q "$container.*Up"; then
            echo "✅ $service - http://localhost:$port"
        else
            echo "❌ $service - No está corriendo"
        fi
    done
    
    echo ""
    echo "🔑 Credenciales por defecto:"
    echo "   • Airflow: admin / admin"
    echo "   • Superset: admin / admin"
    echo "   • PostgreSQL: postgres / FoolishPassword"
}

# Función para mostrar siguiente pasos
show_next_steps() {
    echo ""
    echo "🎯 Próximos pasos:"
    echo "=================="
    echo "1. 🌊 Ir a Airflow: http://localhost:8080"
    echo "   • Usuario: admin, Contraseña: admin"
    echo "   • Buscar DAG: 'vaers_polars_pipeline'"
    echo "   • Activar y ejecutar el DAG"
    echo ""
    echo "2. 📊 Ver progreso:"
    echo "   • Logs de Airflow para seguir el ETL"
    echo "   • Monitor: docker logs monitor_polars -f"
    echo ""
    echo "3. 📈 Ver resultados en Superset: http://localhost:8088"
    echo "   • Usuario: admin, Contraseña: admin"
    echo "   • Los dashboards se crearán automáticamente"
    echo ""
    echo "4. 🐲 Consultar Druid directamente: http://localhost:8888"
    echo ""
    echo "🆘 Si hay problemas:"
    echo "   • Logs: docker compose -f docker-compose-polars.yml logs"
    echo "   • Reiniciar: docker compose -f docker-compose-polars.yml restart"
    echo "   • Limpiar: docker compose -f docker-compose-polars.yml down -v"
}

# Función para verificar que todo esté funcionando
verify_system() {
    echo ""
    echo "🔍 Verificando sistema..."
    sleep 5
    
    # Verificar que los contenedores estén corriendo
    running_containers=$(docker ps --format "{{.Names}}" | grep -E "(polars|postgres)" | wc -l)
    
    if [ "$running_containers" -ge 8 ]; then
        echo "✅ Sistema funcionando correctamente ($running_containers contenedores activos)"
        return 0
    else
        echo "⚠️ Algunos servicios pueden no estar funcionando correctamente"
        echo "🔍 Contenedores activos:"
        docker ps --format "table {{.Names}}\t{{.Status}}" | grep -E "(polars|postgres)"
        return 1
    fi
}

# MAIN EXECUTION
main() {
    clear
    echo "🐻‍❄️ VAERS COVID Analytics con Polars"
    echo "======================================"
    echo ""
    
    # Verificaciones previas
    check_docker
    check_data
    
    echo ""
    echo "❓ ¿Quieres limpiar contenedores anteriores? (recomendado) [Y/n]"
    read -r response
    if [[ ! "$response" =~ ^[Nn]$ ]]; then
        cleanup_old
    fi
    
    echo ""
    echo "🚀 Iniciando pipeline completo..."
    start_services
    
    echo ""
    echo "⏳ Esperando a que todos los servicios estén listos..."
    sleep 30
    
    verify_system
    show_status
    show_next_steps
    
    echo ""
    echo "🎉 ¡Pipeline VAERS con Polars iniciado exitosamente!"
    echo "📊 CSV → 🐻‍❄️ Polars ETL → 🐲 Druid + 🗄️ PostgreSQL → 📈 Superset"
}

# Ejecutar función principal
main "$@"