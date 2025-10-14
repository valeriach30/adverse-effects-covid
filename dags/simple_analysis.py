#!/usr/bin/env python3
"""
Análisis completo VAERS COVID-19
Procesa datos VAERS y genera 4 análisis principales:
1. Síntomas más frecuentes por fabricante
2. Análisis de severidad por edad  
3. Tiempo de aparición de síntomas
4. Distribución geográfica
"""

import pandas as pd
import json
import os
from collections import Counter
import numpy as np

def load_vaers_data(data_path="/opt/airflow/data"):
    """Carga los 3 archivos CSV de VAERS"""
    print("📂 Cargando datos VAERS...")
    
    try:
        vaers_data = pd.read_csv(f"{data_path}/VAERSDATA.csv", low_memory=False)
        vaers_symptoms = pd.read_csv(f"{data_path}/VAERSSYMPTOMS.csv", low_memory=False)
        vaers_vax = pd.read_csv(f"{data_path}/VAERSVAX.csv", low_memory=False)
        
        print(f"✅ Datos cargados:")
        print(f"   - VAERS Data: {len(vaers_data):,} filas")
        print(f"   - VAERS Symptoms: {len(vaers_symptoms):,} filas") 
        print(f"   - VAERS Vax: {len(vaers_vax):,} filas")
        
        return vaers_data, vaers_symptoms, vaers_vax
        
    except Exception as e:
        print(f"❌ Error cargando datos: {str(e)}")
        raise

def clean_and_filter_covid_data(vaers_data, vaers_vax):
    """Limpia datos y filtra solo vacunas COVID-19"""
    print("🧹 Filtrando y limpiando datos COVID-19...")
    
    # Filtrar solo vacunas COVID
    covid_vaccines = vaers_vax[
        vaers_vax['VAX_MANU'].str.contains('PFIZER|MODERNA|JANSSEN', na=False)
    ].copy()
    
    # Limpiar nombres de fabricantes
    covid_vaccines['VAX_MANU_CLEAN'] = covid_vaccines['VAX_MANU'].apply(
        lambda x: 'PFIZER' if 'PFIZER' in str(x).upper() else
                 'MODERNA' if 'MODERNA' in str(x).upper() else
                 'JANSSEN' if 'JANSSEN' in str(x).upper() else 'OTHER'
    )
    
    print(f"✅ Vacunas COVID filtradas: {len(covid_vaccines):,} registros")
    
    # Join con datos principales
    covid_data = vaers_data.merge(covid_vaccines[['VAERS_ID', 'VAX_MANU_CLEAN']], 
                                on='VAERS_ID', how='inner')
    
    print(f"✅ Datos combinados: {len(covid_data):,} registros")
    
    return covid_data, covid_vaccines

def create_symptoms_dataset(covid_data, vaers_symptoms):
    """Crea dataset unpivoted de síntomas para análisis"""
    print("🔄 Procesando síntomas...")
    
    # Join con síntomas
    covid_symptoms = covid_data.merge(vaers_symptoms, on='VAERS_ID', how='inner')
    
    # Crear lista de síntomas (unpivot manual)
    symptoms_list = []
    for _, row in covid_symptoms.iterrows():
        for i in range(1, 6):  # SYMPTOM1 a SYMPTOM5
            symptom = row.get(f'SYMPTOM{i}')
            if pd.notna(symptom) and symptom.strip():
                symptoms_list.append({
                    'VAERS_ID': row['VAERS_ID'],
                    'VAX_MANU_CLEAN': row['VAX_MANU_CLEAN'],
                    'symptom_name': symptom.strip(),
                    'DIED': row.get('DIED', 'N'),
                    'HOSPITAL': row.get('HOSPITAL', 'N'),
                    'ER_VISIT': row.get('ER_VISIT', 'N'),
                    'AGE_YRS': row.get('AGE_YRS', 0),
                    'STATE': row.get('STATE', ''),
                    'NUMDAYS': row.get('NUMDAYS', None),
                    'SEX': row.get('SEX', 'U'),
                    'RECVDATE': row.get('RECVDATE', ''),
                    'VAX_DATE': row.get('VAX_DATE', ''),
                    'ONSET_DATE': row.get('ONSET_DATE', '')
                })
    
    symptoms_df = pd.DataFrame(symptoms_list)
    print(f"✅ Dataset de síntomas creado: {len(symptoms_df):,} registros")
    
    return symptoms_df

def analyze_symptoms_by_manufacturer(symptoms_df):
    """Análisis 1: Síntomas más frecuentes por fabricante"""
    print("\n📊 Análisis 1: Síntomas más frecuentes por fabricante...")
    
    # Agregación por fabricante y síntoma
    symptom_analysis = symptoms_df.groupby(['VAX_MANU_CLEAN', 'symptom_name']).agg({
        'VAERS_ID': ['count', 'nunique'],
        'AGE_YRS': 'mean',
        'DIED': lambda x: sum(x == 'Y'),
        'HOSPITAL': lambda x: sum(x == 'Y'),
        'ER_VISIT': lambda x: sum(x == 'Y')
    }).round(2)
    
    # Renombrar columnas
    symptom_analysis.columns = ['total_reports', 'unique_cases', 'avg_age', 'deaths', 'hospitalizations', 'er_visits']
    symptom_analysis = symptom_analysis.reset_index()
    
    # Calcular tasas
    symptom_analysis['death_rate'] = (symptom_analysis['deaths'] / symptom_analysis['unique_cases'] * 100).round(2)
    symptom_analysis['hospital_rate'] = (symptom_analysis['hospitalizations'] / symptom_analysis['unique_cases'] * 100).round(2)
    
    # Top 10 por cada fabricante
    top_results = {}
    for manufacturer in ['PFIZER', 'MODERNA', 'JANSSEN']:
        top_10 = symptom_analysis[
            symptom_analysis['VAX_MANU_CLEAN'] == manufacturer
        ].nlargest(10, 'total_reports')
        
        top_results[manufacturer] = top_10.to_dict('records')
        
        print(f"\n🔹 Top 10 síntomas para {manufacturer}:")
        for i, row in enumerate(top_10.head().iterrows(), 1):
            data = row[1]
            print(f"   {i}. {data['symptom_name']}: {data['total_reports']} reportes, "
                  f"{data['death_rate']}% muertes, {data['hospital_rate']}% hospitalizaciones")
    
    return symptom_analysis, top_results

def analyze_severity_by_age(covid_data):
    """Análisis 2: Severidad por grupos de edad"""
    print("\n📊 Análisis 2: Severidad por grupos de edad...")
    
    # Crear grupos de edad
    covid_data_age = covid_data.copy()
    covid_data_age = covid_data_age[covid_data_age['AGE_YRS'].notna()]
    
    covid_data_age['age_group'] = pd.cut(
        covid_data_age['AGE_YRS'], 
        bins=[0, 18, 30, 50, 65, 100], 
        labels=['0-17', '18-29', '30-49', '50-64', '65+'],
        include_lowest=True
    )
    
    # Calcular flags de severidad
    covid_data_age['died_flag'] = (covid_data_age['DIED'] == 'Y').astype(int)
    covid_data_age['hospital_flag'] = (covid_data_age['HOSPITAL'] == 'Y').astype(int)
    covid_data_age['er_flag'] = (covid_data_age['ER_VISIT'] == 'Y').astype(int)
    covid_data_age['severe_flag'] = (covid_data_age['died_flag'] | covid_data_age['hospital_flag']).astype(int)
    
    # Análisis por edad y fabricante
    severity_analysis = covid_data_age.groupby(['age_group', 'VAX_MANU_CLEAN']).agg({
        'VAERS_ID': 'count',
        'AGE_YRS': 'mean',
        'died_flag': 'sum',
        'hospital_flag': 'sum', 
        'er_flag': 'sum',
        'severe_flag': 'sum'
    }).round(2)
    
    severity_analysis.columns = ['total_cases', 'avg_age', 'deaths', 'hospitalizations', 'er_visits', 'severe_cases']
    severity_analysis = severity_analysis.reset_index()
    
    # Calcular tasas por 1000 casos
    severity_analysis['death_rate'] = (severity_analysis['deaths'] / severity_analysis['total_cases'] * 1000).round(2)
    severity_analysis['hospital_rate'] = (severity_analysis['hospitalizations'] / severity_analysis['total_cases'] * 1000).round(2)
    severity_analysis['severe_rate'] = (severity_analysis['severe_cases'] / severity_analysis['total_cases'] * 1000).round(2)
    
    # Mostrar resultados
    print("\n🔹 Tasas de severidad por grupo de edad (por 1000 casos):")
    for age_group in ['0-17', '18-29', '30-49', '50-64', '65+']:
        age_data = severity_analysis[severity_analysis['age_group'] == age_group]
        if len(age_data) > 0:
            print(f"\n   {age_group} años:")
            for _, row in age_data.iterrows():
                print(f"     {row['VAX_MANU_CLEAN']}: {row['total_cases']} casos, "
                      f"Severidad: {row['severe_rate']}/1000, Muertes: {row['death_rate']}/1000")
    
    return severity_analysis

def analyze_symptom_onset_timing(symptoms_df):
    """Análisis 3: Tiempo de aparición de síntomas"""
    print("\n📊 Análisis 3: Tiempo de aparición de síntomas...")
    
    # Filtrar casos con datos de tiempo
    onset_data = symptoms_df[
        (symptoms_df['NUMDAYS'].notna()) & 
        (symptoms_df['NUMDAYS'] >= 0) & 
        (symptoms_df['NUMDAYS'] <= 365)  # Filtrar valores extremos
    ].copy()
    
    if len(onset_data) == 0:
        print("⚠️  No hay datos suficientes de tiempo de aparición")
        return pd.DataFrame()
    
    # Análisis por síntoma y fabricante
    onset_analysis = onset_data.groupby(['symptom_name', 'VAX_MANU_CLEAN']).agg({
        'VAERS_ID': 'count',
        'NUMDAYS': ['mean', 'median', 'std', 'min', 'max']
    }).round(2)
    
    # Simplificar columnas
    onset_analysis.columns = ['total_reports', 'avg_days', 'median_days', 'std_days', 'min_days', 'max_days']
    onset_analysis = onset_analysis.reset_index()
    
    # Filtrar síntomas con al menos 10 reportes
    onset_analysis = onset_analysis[onset_analysis['total_reports'] >= 10]
    
    # Top síntomas por aparición rápida y tardía
    print("\n🔹 Top 10 síntomas de aparición más rápida (días promedio):")
    fastest_symptoms = onset_analysis.nsmallest(10, 'avg_days')
    for _, row in fastest_symptoms.iterrows():
        print(f"   {row['symptom_name']} ({row['VAX_MANU_CLEAN']}): "
              f"{row['avg_days']:.1f} días promedio ({row['total_reports']} casos)")
    
    print("\n🔹 Top 10 síntomas de aparición más tardía (días promedio):")
    slowest_symptoms = onset_analysis.nlargest(10, 'avg_days')
    for _, row in slowest_symptoms.iterrows():
        print(f"   {row['symptom_name']} ({row['VAX_MANU_CLEAN']}): "
              f"{row['avg_days']:.1f} días promedio ({row['total_reports']} casos)")
    
    return onset_analysis

def analyze_geographic_distribution(covid_data):
    """Análisis 4: Distribución geográfica"""
    print("\n📊 Análisis 4: Distribución geográfica...")
    
    # Filtrar estados válidos
    geo_data = covid_data[
        (covid_data['STATE'].notna()) & 
        (covid_data['STATE'] != '') & 
        (covid_data['STATE'].str.len() == 2)  # Solo códigos de estado válidos
    ].copy()
    
    # Calcular flags
    geo_data['died_flag'] = (geo_data['DIED'] == 'Y').astype(int)
    geo_data['hospital_flag'] = (geo_data['HOSPITAL'] == 'Y').astype(int)
    geo_data['er_flag'] = (geo_data['ER_VISIT'] == 'Y').astype(int)
    
    # Análisis por estado y fabricante
    geo_analysis = geo_data.groupby(['STATE', 'VAX_MANU_CLEAN']).agg({
        'VAERS_ID': 'count',
        'AGE_YRS': 'mean',
        'died_flag': 'sum',
        'hospital_flag': 'sum',
        'er_flag': 'sum'
    }).round(2)
    
    geo_analysis.columns = ['total_reports', 'avg_age', 'deaths', 'hospitalizations', 'er_visits']
    geo_analysis = geo_analysis.reset_index()
    
    # Calcular tasas
    geo_analysis['death_rate'] = (geo_analysis['deaths'] / geo_analysis['total_reports'] * 100).round(2)
    geo_analysis['hospital_rate'] = (geo_analysis['hospitalizations'] / geo_analysis['total_reports'] * 100).round(2)
    
    # Top estados por reportes totales
    state_totals = geo_analysis.groupby('STATE')['total_reports'].sum().sort_values(ascending=False)
    
    print("\n🔹 Top 15 estados por número de reportes:")
    for i, (state, total) in enumerate(state_totals.head(15).items(), 1):
        state_death_rate = geo_analysis[geo_analysis['STATE'] == state]['death_rate'].mean()
        state_hosp_rate = geo_analysis[geo_analysis['STATE'] == state]['hospital_rate'].mean()
        print(f"   {i}. {state}: {total:,} reportes, "
              f"Muertes: {state_death_rate:.2f}%, Hospitalizaciones: {state_hosp_rate:.2f}%")
    
    return geo_analysis

def save_analysis_results(symptom_analysis, top_results, severity_analysis, 
                         onset_analysis, geo_analysis, covid_data):
    """Guarda todos los resultados de análisis"""
    print("\n💾 Guardando resultados...")
    
    # Usar solo el volumen compartido - crítico para Druid
    output_dir = "/opt/shared_data/vaers_results"
    
    # El directorio debería existir por setup_shared_directories del DAG
    if not os.path.exists(output_dir):
        raise Exception(f"❌ Directorio crítico no existe: {output_dir} - Verificar configuración de volúmenes")
    
    # Verificar que podemos escribir
    try:
        test_file = os.path.join(output_dir, 'test_write.tmp')
        with open(test_file, 'w') as f:
            f.write('test')
        os.remove(test_file)
        print(f"✅ Directorio de resultados verificado: {output_dir}")
    except Exception as e:
        raise Exception(f"❌ No se puede escribir en {output_dir}: {e}")
    
    # 1. Guardar CSVs individuales
    symptom_analysis.to_csv(f"{output_dir}/symptoms_by_manufacturer.csv", index=False)
    severity_analysis.to_csv(f"{output_dir}/severity_by_age.csv", index=False)
    
    if len(onset_analysis) > 0:
        onset_analysis.to_csv(f"{output_dir}/symptom_onset_timing.csv", index=False)
    
    geo_analysis.to_csv(f"{output_dir}/geographic_distribution.csv", index=False)
    
    # 2. Resumen completo JSON
    summary = {
        "analysis_date": pd.Timestamp.now().isoformat(),
        "total_covid_reports": len(covid_data),
        "manufacturers": {
            "PFIZER": len(covid_data[covid_data['VAX_MANU_CLEAN'] == 'PFIZER']),
            "MODERNA": len(covid_data[covid_data['VAX_MANU_CLEAN'] == 'MODERNA']),
            "JANSSEN": len(covid_data[covid_data['VAX_MANU_CLEAN'] == 'JANSSEN'])
        },
        "analysis_results": {
            "top_symptoms_by_manufacturer": top_results,
            "severity_by_age_groups": len(severity_analysis),
            "onset_timing_records": len(onset_analysis),
            "geographic_states_analyzed": geo_analysis['STATE'].nunique()
        }
    }
    
    with open(f"{output_dir}/analysis_summary.json", "w", encoding='utf-8') as f:
        json.dump(summary, f, indent=2, default=str)
    
    # 3. Datos para Druid (NDJSON)
    generate_druid_datasets(symptom_analysis, severity_analysis, onset_analysis, geo_analysis, output_dir)
    
    print(f"✅ Resultados guardados en: {output_dir}/")
    print(f"   - symptoms_by_manufacturer.csv ({len(symptom_analysis)} registros)")
    print(f"   - severity_by_age.csv ({len(severity_analysis)} registros)")
    print(f"   - symptom_onset_timing.csv ({len(onset_analysis)} registros)")
    print(f"   - geographic_distribution.csv ({len(geo_analysis)} registros)")
    print(f"   - analysis_summary.json (resumen completo)")

def generate_druid_datasets(symptom_analysis, severity_analysis, onset_analysis, geo_analysis, output_dir):
    """Genera datasets en formato NDJSON para Druid"""
    
    # Dataset 1: Síntomas por fabricante
    with open(f"{output_dir}/symptoms_for_druid.json", "w", encoding='utf-8') as f:
        for _, row in symptom_analysis.iterrows():
            record = {
                "__time": pd.Timestamp.now().isoformat(),
                "analysis_type": "symptoms_by_manufacturer",
                "VAX_MANU_CLEAN": row['VAX_MANU_CLEAN'],
                "symptom_name": row['symptom_name'],
                "total_reports": int(row['total_reports']),
                "unique_cases": int(row['unique_cases']),
                "deaths": int(row['deaths']),
                "hospitalizations": int(row['hospitalizations']),
                "er_visits": int(row['er_visits']),
                "avg_age": float(row['avg_age']) if pd.notna(row['avg_age']) else 0.0,
                "death_rate": float(row['death_rate']),
                "hospital_rate": float(row['hospital_rate'])
            }
            f.write(json.dumps(record) + "\n")
    
    # Dataset 2: Severidad por edad  
    with open(f"{output_dir}/severity_for_druid.json", "w", encoding='utf-8') as f:
        for _, row in severity_analysis.iterrows():
            record = {
                "__time": pd.Timestamp.now().isoformat(),
                "analysis_type": "severity_by_age",
                "age_group": str(row['age_group']),
                "VAX_MANU_CLEAN": row['VAX_MANU_CLEAN'],
                "total_cases": int(row['total_cases']),
                "avg_age": float(row['avg_age']),
                "deaths": int(row['deaths']),
                "hospitalizations": int(row['hospitalizations']),
                "er_visits": int(row['er_visits']),
                "severe_cases": int(row['severe_cases']),
                "death_rate": float(row['death_rate']),
                "hospital_rate": float(row['hospital_rate']),
                "severe_rate": float(row['severe_rate'])
            }
            f.write(json.dumps(record) + "\n")
    
    # Dataset 3: Distribución geográfica
    with open(f"{output_dir}/geographic_for_druid.json", "w", encoding='utf-8') as f:
        for _, row in geo_analysis.iterrows():
            record = {
                "__time": pd.Timestamp.now().isoformat(),
                "analysis_type": "geographic_distribution",
                "state": row['STATE'],
                "VAX_MANU_CLEAN": row['VAX_MANU_CLEAN'],
                "total_reports": int(row['total_reports']),
                "avg_age": float(row['avg_age']),
                "deaths": int(row['deaths']),
                "hospitalizations": int(row['hospitalizations']),
                "er_visits": int(row['er_visits']),
                "death_rate": float(row['death_rate']),
                "hospital_rate": float(row['hospital_rate'])
            }
            f.write(json.dumps(record) + "\n")

def load_and_analyze_vaers():
    """Función principal - ejecuta todos los análisis VAERS"""
    print("🚀 Iniciando análisis completo VAERS COVID-19...")
    
    try:
        # 1. Cargar datos
        vaers_data, vaers_symptoms, vaers_vax = load_vaers_data()
        
        # 2. Limpiar y filtrar datos COVID
        covid_data, covid_vaccines = clean_and_filter_covid_data(vaers_data, vaers_vax)
        
        # 3. Crear dataset de síntomas
        symptoms_df = create_symptoms_dataset(covid_data, vaers_symptoms)
        
        # 4. Ejecutar los 4 análisis principales
        print("\n" + "="*60)
        
        # Análisis 1: Síntomas por fabricante
        symptom_analysis, top_results = analyze_symptoms_by_manufacturer(symptoms_df)
        
        # Análisis 2: Severidad por edad
        severity_analysis = analyze_severity_by_age(covid_data)
        
        # Análisis 3: Tiempo de aparición
        onset_analysis = analyze_symptom_onset_timing(symptoms_df)
        
        # Análisis 4: Distribución geográfica
        geo_analysis = analyze_geographic_distribution(covid_data)
        
        # 5. Guardar todos los resultados
        save_analysis_results(
            symptom_analysis, top_results, severity_analysis,
            onset_analysis, geo_analysis, covid_data
        )
        
        print("\n" + "="*60)
        print("✅ Análisis completo VAERS finalizado exitosamente!")
        print(f"📊 Análisis generados:")
        print(f"   1. ✅ Síntomas por fabricante ({len(symptom_analysis)} registros)")
        print(f"   2. ✅ Severidad por edad ({len(severity_analysis)} registros)")
        print(f"   3. ✅ Tiempo de aparición ({len(onset_analysis)} registros)")
        print(f"   4. ✅ Distribución geográfica ({len(geo_analysis)} registros)")
        print("="*60)
        
        return True
        
    except Exception as e:
        print(f"❌ Error en análisis completo: {str(e)}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = load_and_analyze_vaers()
    exit(0 if success else 1)