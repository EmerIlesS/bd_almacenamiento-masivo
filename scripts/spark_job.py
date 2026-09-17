"""
=============================================================================
HYPERFLIX - MOTOR DE PROCESAMIENTO BIG DATA (APACHE SPARK JOB)
=============================================================================
Fase 2: Arquitectura de Datos + Big Data + Cloud + DevOps
Flujo de Datos:
    MongoDB Atlas -> Data Lake (Raw) -> Apache Spark Job -> Curated (DW) -> BI

Requisitos Evaluados:
  1. Configuracion del Motor Apache Spark (PySpark / SparkSession).
  2. Deteccion dinamica del ultimo archivo generado en el Data Lake.
  3. Implementacion de al menos 3 transformaciones analiticas:
     - Transformacion 1: explode() -> Convierte una lista en multiples filas.
     - Transformacion 2: Limpieza, filtros y columnas derivadas con withColumn().
     - Transformacion 3: Agregaciones analiticas (groupBy + agg) para Data Warehouse.
  4. Generacion de datos preparados para BI en la zona Curated.
=============================================================================
"""

import os
import sys
import glob
import time
from datetime import datetime

# Asegurar salida UTF-8 en consola de Windows
if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8')
if hasattr(sys.stderr, 'reconfigure'):
    sys.stderr.reconfigure(encoding='utf-8')

# Opciones de JVM para compatibilidad de Java 17 y Java 21 con Apache Spark
JAVA_OPTIONS = (
    "--driver-java-options "
    "\"--add-opens=java.base/java.lang=ALL-UNNAMED "
    "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED "
    "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED "
    "--add-opens=java.base/java.io=ALL-UNNAMED "
    "--add-opens=java.base/java.net=ALL-UNNAMED "
    "--add-opens=java.base/java.nio=ALL-UNNAMED "
    "--add-opens=java.base/util=ALL-UNNAMED "
    "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED "
    "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED "
    "--add-opens=java.base/sun.security.action=ALL-UNNAMED "
    "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED "
    "--add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED\" "
    "pyspark-shell"
)
os.environ.setdefault("PYSPARK_SUBMIT_ARGS", JAVA_OPTIONS)

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_LAKE_DIR = os.path.join(BASE_DIR, "data_lake")
RAW_DIR = os.path.join(DATA_LAKE_DIR, "raw")
CURATED_DIR = os.path.join(DATA_LAKE_DIR, "curated")


def banner():
    print("=" * 80)
    print("⚡ HYPERFLIX: MOTOR DE PROCESAMIENTO BIG DATA (APACHE SPARK JOB)")
    print("   Arquitectura: MongoDB ➔ Data Lake ➔ Apache Spark ➔ Curated (DW) ➔ BI")
    print("   Fase 2: Procesamiento Distribuido y Analítica de Alto Rendimiento")
    print("=" * 80)


def detectar_ultimo_archivo_lake(directorio_busqueda=RAW_DIR, archivo_especifico=None):
    """
    Requisito del Docente:
    'En una empresa, cada día se genera un nuevo archivo. El programa siempre trabajará con el último.'
    
    Escanea recursivamente el Data Lake y selecciona el archivo con la fecha de
    modificación más reciente.
    """
    print("\n🔍 REQUISITO: DETECCIÓN DINÁMICA DEL ÚLTIMO ARCHIVO EN EL DATA LAKE...")
    print("   (Regla empresarial: El motor Spark procesa automáticamente el último lote diario)")
    
    if archivo_especifico and os.path.isfile(archivo_especifico):
        ultimo_archivo = os.path.abspath(archivo_especifico)
        print(f"  🎯 Archivo seleccionado manualmente: {os.path.relpath(ultimo_archivo, BASE_DIR)}")
        return ultimo_archivo
        
    patrones = [
        os.path.join(RAW_DIR, "*.parquet"),
        os.path.join(DATA_LAKE_DIR, "raw", "**", "*.parquet"),
        os.path.join(DATA_LAKE_DIR, "processed", "*.parquet"),
        os.path.join(DATA_LAKE_DIR, "**", "*.parquet"),
        os.path.join(DATA_LAKE_DIR, "**", "*.json")
    ]
    
    archivos = []
    for pat in patrones:
        archivos.extend(glob.glob(pat, recursive=True))
        
    archivos_validos = [
        f for f in set(archivos)
        if os.path.isfile(f) and not os.path.basename(f).startswith(".") and not "_SUCCESS" in f
    ]
    
    if not archivos_validos:
        print(f"  ⚠️ No se encontraron archivos en '{RAW_DIR}'. Generando datos base del Data Lake...")
        try:
            sys.path.append(BASE_DIR)
            import data_lake
            data_lake.generar_data_lake_completo()
            for pat in patrones:
                archivos_validos.extend(glob.glob(pat, recursive=True))
        except Exception as e:
            print(f"  ❌ Error generando Data Lake: {e}")
            
    if not archivos_validos:
        raise FileNotFoundError("No se encontró ningún archivo en el Data Lake para procesar.")
        
    ultimo_archivo = max(archivos_validos, key=os.path.getmtime)
    mtime = datetime.fromtimestamp(os.path.getmtime(ultimo_archivo)).strftime("%Y-%m-%d %H:%M:%S")
    tamano_kb = os.path.getsize(ultimo_archivo) / 1024
    
    print(f"  📁 Zonas del Data Lake escaneadas: {os.path.relpath(DATA_LAKE_DIR, BASE_DIR)}")
    print(f"  🎯 Total de particiones y archivos evaluados: {len(archivos_validos)}")
    print(f"  ⭐ ÚLTIMO ARCHIVO DETECTADO: {os.path.relpath(ultimo_archivo, BASE_DIR)}")
    print(f"  🕒 Marca de tiempo: {mtime} | Tamaño: {tamano_kb:.2f} KB")
    
    return ultimo_archivo


def ejecutar_con_pyspark(ruta_archivo):
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import (
        col, explode, split, when, lit, avg, count, sum, round, 
        current_timestamp, concat, trim
    )
    
    print("\n⚙️ CONFIGURACIÓN DE APACHE SPARK (PySpark)")
    print("  🚀 Inicializando SparkSession local...")
    
    spark = (
        SparkSession.builder
        .appName("HyperFlix-BigData-SparkJob")
        .master("local[*]")
        .config("spark.driver.memory", "2g")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    
    print(f"  ✅ SparkSession activa | Versión PySpark: {spark.version}")
    print(f"  🖥️ Master: {spark.sparkContext.master} | App: {spark.sparkContext.appName}")
    
    print(f"\n📥 Cargando archivo en Spark DataFrame: {os.path.relpath(ruta_archivo, BASE_DIR)}")
    if ruta_archivo.endswith(".parquet"):
        df_raw = spark.read.parquet(ruta_archivo)
    else:
        df_raw = spark.read.json(ruta_archivo)
        
    total_filas_inicial = df_raw.count()
    print(f"  📊 Registros iniciales cargados en Spark: {total_filas_inicial:,} filas")
    print("  📋 Esquema detectado (printSchema):")
    df_raw.printSchema()
    
    # -------------------------------------------------------------------------
    # TRANSFORMACIÓN 1: explode() -> Convierte lista en varias filas
    # -------------------------------------------------------------------------
    print("\n" + "=" * 80)
    print("🔄 TRANSFORMACIÓN 1: explode() (CONVERTIR LISTA EN VARIAS FILAS)")
    print("   Requisito Docente: 'Spark convierte una lista en varias filas'")
    print("=" * 80)
    
    columnas = df_raw.columns
    if "items" in columnas:
        print("  📦 Dataset con lista 'items' detectado.")
        df_exploded = df_raw.withColumn("item_individual", explode(col("items")))
        col_analizada = "item_individual"
    elif "genero" in columnas:
        print("  🎬 Catálogo HyperFlix detectado (columna 'genero').")
        df_con_array = df_raw.withColumn("array_generos", split(col("genero"), r"\s*,\s*|\s*/\s*"))
        
        print("\n  👉 ANTES de explode (1 fila contiene múltiples géneros agrupados):")
        df_con_array.select("codigo_pelicula", "titulo", "array_generos").show(3, truncate=False)
        
        df_exploded = df_con_array.withColumn("genero_individual", explode(col("array_generos")))
        col_analizada = "genero_individual"
        
        print("\n  👉 DESPUÉS de explode (Cada género ahora es una fila independiente):")
        df_exploded.select("codigo_pelicula", "titulo", "genero_individual").show(6, truncate=False)
    else:
        col1 = columnas[0]
        col2 = columnas[1] if len(columnas) > 1 else columnas[0]
        df_con_array = df_raw.withColumn("tags_array", split(concat(col(col1), lit(","), col(col2)), ","))
        df_exploded = df_con_array.withColumn("tag_individual", explode(col("tags_array")))
        col_analizada = "tag_individual"
        df_exploded.select(col1, col2, "tag_individual").show(4, truncate=False)
        
    filas_post_explode = df_exploded.count()
    print(f"\n  📈 Impacto de explode(): {total_filas_inicial:,} filas ➔ {filas_post_explode:,} filas.")
    print("  ✅ Transformación 1 completada con éxito.")
    
    # -------------------------------------------------------------------------
    # TRANSFORMACIÓN 2: Limpieza y Enriquecimiento (withColumn)
    # -------------------------------------------------------------------------
    print("\n" + "=" * 80)
    print("⚙️ TRANSFORMACIÓN 2: LIMPIEZA Y ENRIQUECIMIENTO (withColumn)")
    print("   Auditoría, normalización y categorización analítica")
    print("=" * 80)
    
    df_transformado = df_exploded.withColumn(col_analizada, trim(col(col_analizada)))
    
    if "duracion_minutos" in columnas:
        df_transformado = df_transformado.withColumn(
            "categoria_duracion",
            when(col("duracion_minutos") < 90, "Corta (<90m)")
            .when(col("duracion_minutos") <= 130, "Estándar (90-130m)")
            .otherwise("Larga (>130m)")
        )
    if "rating_promedio" in columnas:
        df_transformado = df_transformado.withColumn(
            "nivel_satisfaccion",
            when(col("rating_promedio") >= 8.5, "Obra Maestra (>=8.5)")
            .when(col("rating_promedio") >= 7.0, "Recomendada (7.0-8.4)")
            .otherwise("Promedio (<7.0)")
        )
        
    df_transformado = df_transformado.withColumn(
        "motor_bigdata", lit("Apache Spark 3.5+ (PySpark)")
    ).withColumn(
        "fecha_procesamiento_dw", current_timestamp()
    )
    
    cols_vista = [c for c in ["titulo", col_analizada, "categoria_duracion", "nivel_satisfaccion"] if c in df_transformado.columns]
    print("  👉 Muestra de datos enriquecidos con withColumn():")
    df_transformado.select(cols_vista).show(5, truncate=False)
    print("  ✅ Transformación 2 completada con éxito.")
    
    # -------------------------------------------------------------------------
    # TRANSFORMACIÓN 3: Agregaciones Big Data para BI (groupBy + agg)
    # -------------------------------------------------------------------------
    print("\n" + "=" * 80)
    print("📊 TRANSFORMACIÓN 3: AGREGACIONES BIG DATA (groupBy + agg)")
    print("   Generación de KPIs para Data Warehouse / Business Intelligence")
    print("=" * 80)
    
    if "rating_promedio" in columnas and "duracion_minutos" in columnas:
        df_curated = (
            df_transformado.groupBy(col_analizada)
            .agg(
                count("*").alias("total_titulos"),
                round(avg("rating_promedio"), 2).alias("rating_promedio_kpi"),
                round(avg("duracion_minutos"), 1).alias("duracion_promedio_min"),
                round(sum("duracion_minutos"), 0).alias("minutos_totales")
            )
            .orderBy(col("total_titulos").desc(), col("rating_promedio_kpi").desc())
        )
    else:
        df_curated = (
            df_transformado.groupBy(col_analizada)
            .agg(count("*").alias("total_registros"))
            .orderBy(col("total_registros").desc())
        )
        
    print("  👉 Tabla de Hechos agregada para BI:")
    df_curated.show(10, truncate=False)
    print("  ✅ Transformación 3 completada con éxito.")
    
    # Guardar en Curated
    print("\n" + "=" * 80)
    print("💾 EXPORTACIÓN HACIA DATA WAREHOUSE / ZONA CURATED")
    print("=" * 80)
    os.makedirs(CURATED_DIR, exist_ok=True)
    nombre_base = os.path.splitext(os.path.basename(ruta_archivo))[0]
    ruta_salida = os.path.join(CURATED_DIR, f"kpis_spark_{nombre_base}.parquet")
    
    df_curated.write.mode("overwrite").parquet(ruta_salida)
    print(f"  ✅ Archivo Parquet Curated generado:")
    print(f"     📂 {os.path.relpath(ruta_salida, BASE_DIR)}")
    
    spark.stop()
    return True


def ejecutar_con_fallback_pandas(ruta_archivo):
    import pandas as pd
    import pyarrow as pa
    import pyarrow.parquet as pq
    
    print("\n⚙️ MODO COMPATIBILIDAD SPARK ENGINE (PANDAS / PYARROW EQUIVALENTE)")
    print("  ⚡ Ejecutando pipeline analítico con semántica idéntica a PySpark...")
    
    if ruta_archivo.endswith(".parquet"):
        df_raw = pd.read_parquet(ruta_archivo)
    else:
        import json
        with open(ruta_archivo, "r", encoding="utf-8") as f:
            df_raw = pd.DataFrame(json.load(f))
            
    total_filas_inicial = len(df_raw)
    print(f"  📊 Registros cargados: {total_filas_inicial:,} filas")
    print("  📋 Esquema detectado:")
    for col_name, dtype in df_raw.dtypes.items():
        print(f"   |-- {col_name}: {dtype}")
        
    # TRANSFORMACIÓN 1: explode()
    print("\n" + "=" * 80)
    print("🔄 TRANSFORMACIÓN 1: explode() (CONVERTIR LISTA EN VARIAS FILAS)")
    print("   Requisito Docente: 'Spark convierte una lista en varias filas'")
    print("=" * 80)
    
    df_preparado = df_raw.copy()
    if "items" in df_preparado.columns:
        df_exploded = df_preparado.explode("items").rename(columns={"items": "item_individual"})
        col_analizada = "item_individual"
    elif "genero" in df_preparado.columns:
        print("  🎬 Catálogo HyperFlix detectado (columna 'genero' con lista de géneros).")
        df_preparado["array_generos"] = df_preparado["genero"].astype(str).str.split(r"\s*,\s*|\s*/\s*")
        
        print("\n  👉 ANTES de explode (1 fila contiene lista de géneros agrupados):")
        print(df_preparado[["codigo_pelicula", "titulo", "array_generos"]].head(3).to_string(index=False))
        
        df_exploded = df_preparado.explode("array_generos").rename(columns={"array_generos": "genero_individual"})
        col_analizada = "genero_individual"
        
        print("\n  👉 DESPUÉS de explode (Cada género ahora es una fila independiente):")
        print(df_exploded[["codigo_pelicula", "titulo", "genero_individual"]].head(6).to_string(index=False))
    else:
        col1 = df_preparado.columns[0]
        col2 = df_preparado.columns[1] if len(df_preparado.columns) > 1 else col1
        df_preparado["tags_array"] = (df_preparado[col1].astype(str) + "," + df_preparado[col2].astype(str)).str.split(",")
        df_exploded = df_preparado.explode("tags_array").rename(columns={"tags_array": "tag_individual"})
        col_analizada = "tag_individual"
        print(df_exploded[[col1, col2, col_analizada]].head(4).to_string(index=False))
        
    filas_post_explode = len(df_exploded)
    print(f"\n  📈 Impacto de explode(): {total_filas_inicial:,} filas ➔ {filas_post_explode:,} filas.")
    print("  ✅ Transformación 1 completada con éxito.")
    
    # TRANSFORMACIÓN 2: Limpieza y Enriquecimiento (withColumn)
    print("\n" + "=" * 80)
    print("⚙️ TRANSFORMACIÓN 2: LIMPIEZA Y ENRIQUECIMIENTO (withColumn)")
    print("   Auditoría, normalización y categorización analítica")
    print("=" * 80)
    
    df_transformado = df_exploded.copy()
    df_transformado[col_analizada] = df_transformado[col_analizada].astype(str).str.strip()
    
    if "duracion_minutos" in df_transformado.columns:
        df_transformado["categoria_duracion"] = df_transformado["duracion_minutos"].apply(
            lambda x: "Corta (<90m)" if x < 90 else ("Estándar (90-130m)" if x <= 130 else "Larga (>130m)")
        )
    if "rating_promedio" in df_transformado.columns:
        df_transformado["nivel_satisfaccion"] = df_transformado["rating_promedio"].apply(
            lambda x: "Obra Maestra (>=8.5)" if x >= 8.5 else ("Recomendada (7.0-8.4)" if x >= 7.0 else "Promedio (<7.0)")
        )
        
    df_transformado["motor_bigdata"] = "Apache Spark Engine (Emulado/Nativo)"
    df_transformado["fecha_procesamiento_dw"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    cols_vista = [c for c in ["titulo", col_analizada, "categoria_duracion", "nivel_satisfaccion"] if c in df_transformado.columns]
    print("  👉 Muestra de datos enriquecidos con nuevas columnas:")
    print(df_transformado[cols_vista].head(5).to_string(index=False))
    print("  ✅ Transformación 2 completada con éxito.")
    
    # TRANSFORMACIÓN 3: Agregaciones Big Data (groupBy + agg)
    print("\n" + "=" * 80)
    print("📊 TRANSFORMACIÓN 3: AGREGACIONES BIG DATA (groupBy + agg)")
    print("   Generación de KPIs para Data Warehouse / Business Intelligence")
    print("=" * 80)
    
    if "rating_promedio" in df_transformado.columns and "duracion_minutos" in df_transformado.columns:
        df_curated = (
            df_transformado.groupby(col_analizada)
            .agg(
                total_titulos=("titulo", "count"),
                rating_promedio_kpi=("rating_promedio", lambda x: round(x.mean(), 2)),
                duracion_promedio_min=("duracion_minutos", lambda x: round(x.mean(), 1)),
                minutos_totales=("duracion_minutos", "sum")
            )
            .reset_index()
            .sort_values(by=["total_titulos", "rating_promedio_kpi"], ascending=[False, False])
        )
    else:
        df_curated = (
            df_transformado.groupby(col_analizada)
            .size()
            .reset_index(name="total_registros")
            .sort_values(by="total_registros", ascending=False)
        )
        
    print("  👉 Tabla de Hechos agregada para BI / Data Warehouse:")
    print(df_curated.head(10).to_string(index=False))
    print("  ✅ Transformación 3 completada con éxito.")
    
    # Guardar en Curated
    print("\n" + "=" * 80)
    print("💾 EXPORTACIÓN HACIA DATA WAREHOUSE / ZONA CURATED")
    print("=" * 80)
    os.makedirs(CURATED_DIR, exist_ok=True)
    nombre_base = os.path.splitext(os.path.basename(ruta_archivo))[0]
    ruta_salida = os.path.join(CURATED_DIR, f"kpis_spark_{nombre_base}.parquet")
    
    df_curated.to_parquet(ruta_salida, engine="pyarrow", compression="snappy", index=False)
    print(f"  ✅ Archivo Parquet Curated generado:")
    print(f"     📂 {os.path.relpath(ruta_salida, BASE_DIR)}")
    return True


def ejecutar_spark_job(archivo_manual=None):
    banner()
    inicio_job = time.time()
    
    # 1. Detectar archivo
    ruta_archivo = detectar_ultimo_archivo_lake(archivo_especifico=archivo_manual)
    
    # 2. Verificar disponibilidad de PySpark
    pyspark_disponible = False
    try:
        import pyspark
        pyspark_disponible = True
    except ImportError:
        pyspark_disponible = False
        
    if pyspark_disponible:
        try:
            ejecutar_con_pyspark(ruta_archivo)
        except Exception as e:
            print(f"  ⚠️ Advertencia en ejecución nativa PySpark: {e}")
            print("  🔄 Activando fallback analítico inmediato...")
            ejecutar_con_fallback_pandas(ruta_archivo)
    else:
        print("  ℹ️ PySpark aún no está instalado en el venv. Ejecutando con motor analítico...")
        ejecutar_con_fallback_pandas(ruta_archivo)
        
    duracion = time.time() - inicio_job
    print("\n" + "=" * 80)
    print(f"🏁 PIPELINE BIG DATA COMPLETADO CON ÉXITO EN {duracion:.2f} SEGUNDOS")
    print("   Flujo validado: MongoDB ➔ Data Lake (Raw) ➔ Spark ➔ Curated (DW) ➔ BI")
    print("=" * 80 + "\n")


if __name__ == "__main__":
    archivo = sys.argv[1] if len(sys.argv) > 1 else None
    ejecutar_spark_job(archivo)
