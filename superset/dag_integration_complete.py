#!/usr/bin/env python3
"""
Script de integración final para DAG - Dashboard completo
"""

import os
import subprocess
import sys

def main():
    """Ejecutar dashboard completo desde DAG"""
    print("🚀 INTEGRACIÓN DAG - Dashboard VAERS Completo")
    print("="*50)
    
    # Detectar entorno
    if os.path.exists('/opt/airflow'):
        script_path = '/opt/airflow/superset/dashboard_complete_fixed.py'
        print("📍 Ejecutándose desde contenedor Airflow")
    else:
        script_path = './superset/dashboard_complete_fixed.py'
        print("📍 Ejecutándose desde host local")
    
    # Verificar que existe el script
    if not os.path.exists(script_path):
        print(f"❌ Script no encontrado: {script_path}")
        return False
    
    try:
        print("⚡ Ejecutando dashboard builder completo...")
        
        # Ejecutar script con mejor manejo de output
        process = subprocess.run([
            sys.executable, script_path
        ], capture_output=True, text=True, timeout=300)  # 5 min timeout
        
        # Mostrar resultado
        if process.returncode == 0:
            print("✅ DASHBOARD COMPLETO CREADO EXITOSAMENTE!")
            print("\n📊 VISUALIZACIONES INCLUIDAS:")
            print("   • 📊 Distribución por fabricantes de vacunas")
            print("   • 📈 Top síntomas más reportados")
            print("   • 🏥 Hospitalizaciones por grupo de edad")
            print("   • 🗺️ Distribución geográfica por estados")
            print("\n🔗 ACCESO AL DASHBOARD:")
            print("   URL: http://localhost:8088")
            print("   Login: admin / admin")
            print("\n" + "="*50)
            
            # Mostrar output del script
            if process.stdout:
                print("📋 DETALLES DE EJECUCIÓN:")
                print(process.stdout[-500:])  # Últimos 500 caracteres
                
            return True
        else:
            print("❌ ERROR EN CREACIÓN DE DASHBOARD")
            print("Error details:", process.stderr)
            if process.stdout:
                print("Output:", process.stdout)
            return False
            
    except subprocess.TimeoutExpired:
        print("❌ Timeout: El script tardó más de 5 minutos")
        return False
    except Exception as e:
        print(f"❌ Error ejecutando script: {str(e)}")
        return False

if __name__ == "__main__":
    success = main()
    
    if success:
        print("\n🎉 INTEGRACIÓN COMPLETADA EXITOSAMENTE! 🎉")
        print("El dashboard VAERS está listo para usar")
    else:
        print("\n💥 ERROR EN INTEGRACIÓN")
        print("Revisar logs para más detalles")
    
    sys.exit(0 if success else 1)