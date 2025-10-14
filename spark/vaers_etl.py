#!/usr/bin/env python3
"""
VAERS Data ETL Pipeline with Spark
Procesa los datos de VAERS y genera análisis para Druid
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import sys
import os

def create_spark_session(app_name="VAERS_ETL"):
    """Crea sesión de Spark con configuraciones optimizadas"""
    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()

def load_vaers_data(spark, data_path="/opt/shared_data"):
    """Carga los tres archivos CSV de VAERS"""
    print("📂 Cargando datos de VAERS...")
    
    # Esquemas para optimizar la carga
    vaers_data_schema = StructType([
        StructField("VAERS_ID", StringType(), True),
        StructField("RECVDATE", StringType(), True),
        StructField("STATE", StringType(), True),
        StructField("AGE_YRS", DoubleType(), True),
        StructField("CAGE_YR", DoubleType(), True),
        StructField("CAGE_MO", DoubleType(), True),
        StructField("SEX", StringType(), True),
        StructField("RPT_DATE", StringType(), True),
        StructField("SYMPTOM_TEXT", StringType(), True),
        StructField("DIED", StringType(), True),
        StructField("DATEDIED", StringType(), True),
        StructField("L_THREAT", StringType(), True),
        StructField("ER_VISIT", StringType(), True),
        StructField("HOSPITAL", StringType(), True),
        StructField("HOSPDAYS", DoubleType(), True),
        StructField("X_STAY", StringType(), True),
        StructField("DISABLE", StringType(), True),
        StructField("RECOVD", StringType(), True),
        StructField("VAX_DATE", StringType(), True),
        StructField("ONSET_DATE", StringType(), True),
        StructField("NUMDAYS", DoubleType(), True),
        StructField("LAB_DATA", StringType(), True),
        StructField("V_ADMINBY", StringType(), True),
        StructField("V_FUNDBY", StringType(), True),
        StructField("OTHER_MEDS", StringType(), True),
        StructField("CUR_ILL", StringType(), True),
        StructField("HISTORY", StringType(), True),
        StructField("PRIOR_VAX", StringType(), True),
        StructField("SPLTTYPE", StringType(), True),
        StructField("FORM_VERS", StringType(), True),
        StructField("TODAYS_DATE", StringType(), True),
        StructField("BIRTH_DEFECT", StringType(), True),
        StructField("OFC_VISIT", StringType(), True),
        StructField("ER_ED_VISIT", StringType(), True),
        StructField("ALLERGIES", StringType(), True)
    ])
    
    # Cargar archivos CSV
    vaers_data = spark.read.csv(f"{data_path}/VAERSDATA.csv", 
                               header=True, 
                               schema=vaers_data_schema,
                               timestampFormat="MM/dd/yyyy")
    
    vaers_symptoms = spark.read.csv(f"{data_path}/VAERSSYMPTOMS.csv", 
                                   header=True, 
                                   inferSchema=True)
    
    vaers_vax = spark.read.csv(f"{data_path}/VAERSVAX.csv", 
                              header=True, 
                              inferSchema=True)
    
    print(f"✅ Datos cargados:")
    print(f"   - VAERS Data: {vaers_data.count():,} filas")
    print(f"   - VAERS Symptoms: {vaers_symptoms.count():,} filas") 
    print(f"   - VAERS Vax: {vaers_vax.count():,} filas")
    
    return vaers_data, vaers_symptoms, vaers_vax

def clean_and_transform_data(vaers_data, vaers_symptoms, vaers_vax):
    """Limpia y transforma los datos"""
    print("🧹 Limpiando y transformando datos...")
    
    # Limpiar datos principales
    vaers_data_clean = vaers_data \
        .filter(col("VAERS_ID").isNotNull()) \
        .withColumn("RECVDATE", to_date(col("RECVDATE"), "MM/dd/yyyy")) \
        .withColumn("VAX_DATE", to_date(col("VAX_DATE"), "MM/dd/yyyy")) \
        .withColumn("ONSET_DATE", to_date(col("ONSET_DATE"), "MM/dd/yyyy")) \
        .withColumn("DIED_FLAG", when(col("DIED") == "Y", 1).otherwise(0)) \
        .withColumn("HOSPITAL_FLAG", when(col("HOSPITAL") == "Y", 1).otherwise(0)) \
        .withColumn("ER_VISIT_FLAG", when(col("ER_VISIT") == "Y", 1).otherwise(0)) \
        .withColumn("L_THREAT_FLAG", when(col("L_THREAT") == "Y", 1).otherwise(0))
    
    # Limpiar datos de vacunas - filtrar COVID vaccines
    covid_manufacturers = ["PFIZER\\BIONTECH", "MODERNA", "JANSSEN"]
    vaers_vax_clean = vaers_vax \
        .filter(col("VAERS_ID").isNotNull()) \
        .filter(col("VAX_MANU").isin(covid_manufacturers)) \
        .withColumn("VAX_MANU_CLEAN", 
                   when(col("VAX_MANU").contains("PFIZER"), "PFIZER")
                   .when(col("VAX_MANU").contains("MODERNA"), "MODERNA")  
                   .when(col("VAX_MANU").contains("JANSSEN"), "JANSSEN")
                   .otherwise("OTHER"))
    
    # Transformar síntomas - unpivot para análisis
    symptoms_unpivot = vaers_symptoms \
        .filter(col("VAERS_ID").isNotNull()) \
        .select(
            col("VAERS_ID"),
            stack(5, 
                  lit("SYMPTOM1"), col("SYMPTOM1"),
                  lit("SYMPTOM2"), col("SYMPTOM2"), 
                  lit("SYMPTOM3"), col("SYMPTOM3"),
                  lit("SYMPTOM4"), col("SYMPTOM4"),
                  lit("SYMPTOM5"), col("SYMPTOM5")
                 ).alias("symptom_type", "symptom_name")
        ) \
        .filter(col("symptom_name").isNotNull() & (col("symptom_name") != ""))
    
    print("✅ Datos transformados exitosamente")
    return vaers_data_clean, vaers_vax_clean, symptoms_unpivot

def analyze_symptoms_by_manufacturer(vaers_data, vaers_vax, symptoms_unpivot):
    """Análisis 1: Síntomas más frecuentes por fabricante"""
    print("📊 Análisis 1: Síntomas más frecuentes por fabricante...")
    
    # Join de todas las tablas
    analysis_df = vaers_data \
        .select("VAERS_ID", "AGE_YRS", "SEX", "STATE", "DIED_FLAG", 
                "HOSPITAL_FLAG", "ER_VISIT_FLAG", "L_THREAT_FLAG", "NUMDAYS") \
        .join(vaers_vax.select("VAERS_ID", "VAX_MANU_CLEAN"), "VAERS_ID", "inner") \
        .join(symptoms_unpivot, "VAERS_ID", "inner")
    
    # Top 10 síntomas por fabricante
    top_symptoms = analysis_df \
        .groupBy("VAX_MANU_CLEAN", "symptom_name") \
        .agg(
            count("*").alias("total_reports"),
            countDistinct("VAERS_ID").alias("unique_cases"),
            avg("AGE_YRS").alias("avg_age"),
            sum("DIED_FLAG").alias("deaths"),
            sum("HOSPITAL_FLAG").alias("hospitalizations"),
            sum("ER_VISIT_FLAG").alias("er_visits")
        ) \
        .withColumn("death_rate", col("deaths") / col("unique_cases") * 100) \
        .withColumn("hospital_rate", col("hospitalizations") / col("unique_cases") * 100)
    
    # Guardar resultado
    output_path = "/opt/shared_data/symptoms_by_manufacturer"
    top_symptoms.write.mode("overwrite").parquet(output_path)
    
    # Mostrar top 10 por cada fabricante
    for manufacturer in ["PFIZER", "MODERNA", "JANSSEN"]:
        print(f"\n🔹 Top 10 síntomas para {manufacturer}:")
        top_10 = top_symptoms \
            .filter(col("VAX_MANU_CLEAN") == manufacturer) \
            .orderBy(col("total_reports").desc()) \
            .limit(10)
        top_10.show(truncate=False)
    
    return top_symptoms

def analyze_severity_by_age(vaers_data, vaers_vax):
    """Análisis 2: Severidad por edad"""
    print("📊 Análisis 2: Análisis de severidad por edad...")
    
    severity_analysis = vaers_data \
        .join(vaers_vax.select("VAERS_ID", "VAX_MANU_CLEAN"), "VAERS_ID", "inner") \
        .filter(col("AGE_YRS").isNotNull()) \
        .withColumn("age_group", 
                   when(col("AGE_YRS") < 18, "0-17")
                   .when(col("AGE_YRS") < 30, "18-29")
                   .when(col("AGE_YRS") < 50, "30-49") 
                   .when(col("AGE_YRS") < 65, "50-64")
                   .otherwise("65+")) \
        .groupBy("age_group", "VAX_MANU_CLEAN") \
        .agg(
            count("*").alias("total_reports"),
            avg("AGE_YRS").alias("avg_age"),
            sum("DIED_FLAG").alias("deaths"),
            sum("HOSPITAL_FLAG").alias("hospitalizations"),
            sum("ER_VISIT_FLAG").alias("er_visits"),
            sum("L_THREAT_FLAG").alias("life_threatening")
        ) \
        .withColumn("death_rate", col("deaths") / col("total_reports") * 100) \
        .withColumn("hospital_rate", col("hospitalizations") / col("total_reports") * 100) \
        .withColumn("er_rate", col("er_visits") / col("total_reports") * 100)
    
    # Guardar resultado
    output_path = "/opt/shared_data/severity_by_age"
    severity_analysis.write.mode("overwrite").parquet(output_path)
    
    print("\n🔹 Análisis de severidad por grupo de edad:")
    severity_analysis.orderBy("age_group", "VAX_MANU_CLEAN").show()
    
    return severity_analysis

def analyze_symptom_onset_time(vaers_data, vaers_vax, symptoms_unpivot):
    """Análisis 3: Tiempo de aparición de síntomas"""
    print("📊 Análisis 3: Tiempo de aparición de síntomas...")
    
    onset_analysis = vaers_data \
        .join(vaers_vax.select("VAERS_ID", "VAX_MANU_CLEAN"), "VAERS_ID", "inner") \
        .join(symptoms_unpivot, "VAERS_ID", "inner") \
        .filter(col("NUMDAYS").isNotNull() & (col("NUMDAYS") >= 0)) \
        .groupBy("symptom_name", "VAX_MANU_CLEAN") \
        .agg(
            count("*").alias("total_reports"),
            avg("NUMDAYS").alias("avg_days_to_onset"),
            min("NUMDAYS").alias("min_days"),
            max("NUMDAYS").alias("max_days"),
            expr("percentile_approx(NUMDAYS, 0.5)").alias("median_days")
        ) \
        .filter(col("total_reports") >= 10)  # Solo síntomas con suficientes reportes
    
    # Guardar resultado
    output_path = "/opt/shared_data/symptom_onset_time"
    onset_analysis.write.mode("overwrite").parquet(output_path)
    
    print("\n🔹 Top 15 síntomas por tiempo promedio de aparición:")
    onset_analysis \
        .orderBy(col("total_reports").desc()) \
        .limit(15) \
        .show()
    
    return onset_analysis

def analyze_geographic_distribution(vaers_data, vaers_vax):
    """Análisis 4: Distribución geográfica"""
    print("📊 Análisis 4: Distribución geográfica de reportes...")
    
    geo_analysis = vaers_data \
        .join(vaers_vax.select("VAERS_ID", "VAX_MANU_CLEAN"), "VAERS_ID", "inner") \
        .filter(col("STATE").isNotNull()) \
        .groupBy("STATE", "VAX_MANU_CLEAN") \
        .agg(
            count("*").alias("total_reports"),
            countDistinct("VAERS_ID").alias("unique_cases"),
            sum("DIED_FLAG").alias("deaths"),
            sum("HOSPITAL_FLAG").alias("hospitalizations")
        ) \
        .withColumn("death_rate", col("deaths") / col("unique_cases") * 100) \
        .withColumn("hospital_rate", col("hospitalizations") / col("unique_cases") * 100)
    
    # Guardar resultado
    output_path = "/opt/shared_data/geographic_distribution"
    geo_analysis.write.mode("overwrite").parquet(output_path)
    
    print("\n🔹 Top 15 estados por número de reportes:")
    geo_analysis \
        .groupBy("STATE") \
        .agg(sum("total_reports").alias("state_total")) \
        .orderBy(col("state_total").desc()) \
        .limit(15) \
        .show()
    
    return geo_analysis

def main():
    """Función principal del pipeline ETL"""
    print("🚀 Iniciando pipeline VAERS ETL...")
    
    # Crear sesión Spark
    spark = create_spark_session()
    
    try:
        # 1. Cargar datos
        vaers_data, vaers_symptoms, vaers_vax = load_vaers_data(spark)
        
        # 2. Limpiar y transformar
        vaers_data_clean, vaers_vax_clean, symptoms_unpivot = clean_and_transform_data(
            vaers_data, vaers_symptoms, vaers_vax
        )
        
        # 3. Ejecutar análisis
        print("\n" + "="*60)
        analyze_symptoms_by_manufacturer(vaers_data_clean, vaers_vax_clean, symptoms_unpivot)
        
        print("\n" + "="*60) 
        analyze_severity_by_age(vaers_data_clean, vaers_vax_clean)
        
        print("\n" + "="*60)
        analyze_symptom_onset_time(vaers_data_clean, vaers_vax_clean, symptoms_unpivot)
        
        print("\n" + "="*60)
        analyze_geographic_distribution(vaers_data_clean, vaers_vax_clean)
        
        print("\n✅ Pipeline completado exitosamente!")
        
    except Exception as e:
        print(f"❌ Error en el pipeline: {str(e)}")
        raise e
    finally:
        spark.stop()

if __name__ == "__main__":
    main()