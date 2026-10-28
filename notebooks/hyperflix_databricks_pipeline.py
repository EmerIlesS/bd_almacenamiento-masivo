# Databricks notebook source
# MAGIC %md
# MAGIC # 🎬 HYPERFLIX: Pipeline de Procesamiento Distribuido Big Data
# MAGIC ### Arquitectura: Data Lake (DBFS) ➔ Apache Spark Engine ➔ Curated Layer ➔ BI
# MAGIC **Asignatura:** Bases de Datos y Almacenamiento Masivo (Octavo Semestre)  
# MAGIC **Entorno:** Databricks Community Edition (Apache Spark 3.5+ en clúster Linux distribuido)  
# MAGIC **Autor:** Emerson Iles — Ingeniería de Sistemas 

# COMMAND ----------
# MAGIC %md
# MAGIC ## 📌 1. Contexto de Arquitectura Cloud vs Local
# MAGIC En el entorno de desarrollo local (Windows) se ejecutó PySpark en modo `local[*]` utilizando un patrón híbrido para sortear las restricciones del sistema de archivos de Hadoop (`winutils.exe`).
# MAGIC 
# MAGIC En esta versión **Cloud Enterprise (Databricks)**:
# MAGIC 1. Apache Spark corre nativamente sobre nodos Linux distribuidos.
# MAGIC 2. Se elimina cualquier capa intermedia y se utiliza escritura nativa en **DBFS (Databricks File System)** y formato **Delta Lake / Parquet**.
# MAGIC 3. El procesamiento escala elásticamente a millones de eventos de streaming.

# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.types import *

print("⚡ Versión activa de Apache Spark en Databricks:", spark.version)
print("💻 Clúster conectado:", spark.sparkContext.master)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 📥 2. Ingesta desde Zona Raw a Spark DataFrame
# MAGIC Simulamos la ingesta de telemetría de streaming y catálogo audiovisual de HYPERFLIX directamente en el clúster.

# COMMAND ----------
# Datos muestra de telemetría y catálogo de HYPERFLIX para procesamiento distribuido
datos_streaming = [
    ("EVT-001", "MOV-0101", "El Padrino", "Crimen, Drama", 175, 160, "SMART_TV", "AndroidTV", 4500, 22.5, 0),
    ("EVT-002", "MOV-0102", "Inception", "Acción, Ciencia Ficción", 148, 145, "LAPTOP", "Windows", 5800, 18.0, 1),
    ("EVT-003", "MOV-0103", "Interstellar", "Ciencia Ficción, Drama", 169, 45, "SMARTPHONE", "iOS", 3200, 45.0, 3),
    ("EVT-004", "MOV-0104", "The Dark Knight", "Acción, Crimen, Drama", 152, 150, "SMART_TV", "Tizen", 6200, 15.0, 0),
    ("EVT-005", "MOV-0105", "Pulp Fiction", "Crimen, Comedia", 154, 154, "TABLET", "iPadOS", 4100, 28.0, 0),
    ("EVT-006", "MOV-0106", "Matrix", "Acción, Ciencia Ficción", 136, 130, "SMART_TV", "WebOS", 5400, 19.5, 0),
    ("EVT-007", "MOV-0107", "Gladiador", "Acción, Aventura, Drama", 155, 60, "SMARTPHONE", "Android", 2900, 55.0, 4),
    ("EVT-008", "MOV-0108", "Parasite", "Drama, Suspenso", 132, 130, "LAPTOP", "macOS", 4900, 21.0, 1),
    ("EVT-009", "MOV-0109", "Avatar", "Acción, Aventura, Fantasía", 162, 160, "SMART_TV", "AndroidTV", 6800, 14.0, 0),
    ("EVT-010", "MOV-0110", "Coco", "Animación, Aventura, Familiar", 105, 105, "SMART_TV", "Roku", 4200, 20.0, 0)
]

columnas = [
    "evento_id", "codigo_pelicula", "titulo", "generos_str", "duracion_min",
    "minutos_vistos", "tipo_dispositivo", "sistema_operativo", "bitrate_kbps", 
    "latencia_ms", "buffering_veces"
]

df_raw = spark.createDataFrame(datos_streaming, columnas)

print(f"📊 Registros ingestados en Spark: {df_raw.count()} eventos")
display(df_raw)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 🔄 3. Transformación 1: `explode()` (Normalización de Listas a Filas)
# MAGIC **Requisito ABP:** Descomponer cadenas o arrays de múltiples categorías en filas individuales para análisis columnar desagregado.

# COMMAND ----------
# 1. Convertir la cadena de géneros separados por coma en un Array
df_con_array = df_raw.withColumn("array_generos", F.split(F.col("generos_str"), r"\s*,\s*"))

# 2. Aplicar explode(): Cada género ahora se convierte en un registro individual
df_exploded = df_con_array.withColumn("genero_individual", F.explode(F.col("array_generos")))

print("📈 Registros antes de explode:", df_raw.count())
print("📈 Registros después de explode:", df_exploded.count())

display(df_exploded.select("evento_id", "titulo", "generos_str", "genero_individual"))

# COMMAND ----------
# MAGIC %md
# MAGIC ## ⚙️ 4. Transformación 2: Limpieza, Enriquecimiento y QoS (`withColumn`)
# MAGIC Calculamos métricas de negocio de streaming:
# MAGIC * **Porcentaje de completitud:** `(minutos_vistos / duracion_min) * 100`
# MAGIC * **Abandono prematuro:** Flag si el usuario vio menos del 20%
# MAGIC * **Score de Calidad de Servicio (QoS):** Penalizado por latencia y buffering

# COMMAND ----------
df_enriquecido = (
    df_exploded
    # 1. Porcentaje de completitud
    .withColumn("completitud_pct", F.round((F.col("minutos_vistos") / F.col("duracion_min")) * 100, 2))
    
    # 2. Detección de abandono (Churn prematuro)
    .withColumn("abandono_prematuro", F.when(F.col("completitud_pct") < 20.0, F.lit(True)).otherwise(F.lit(False)))
    
    # 3. Categorización de duración
    .withColumn(
        "categoria_duracion",
        F.when(F.col("duracion_min") < 120, "Corta / Estándar (<2h)")
        .otherwise("Larga (>2h)")
    )
    
    # 4. Cálculo sintético de Calidad de Servicio QoS (0 a 100)
    .withColumn(
        "score_qos",
        F.round(F.greatest(F.lit(0.0), F.lit(100.0) - (F.col("latencia_ms") * 0.4) - (F.col("buffering_veces") * 10.0)), 1)
    )
    
    # 5. Metadata de auditoría
    .withColumn("procesado_por", F.lit("Databricks Cloud Engine"))
    .withColumn("fecha_proceso", F.current_timestamp())
)

display(df_enriquecido.select(
    "titulo", "genero_individual", "tipo_dispositivo", 
    "completitud_pct", "abandono_prematuro", "score_qos"
))

# COMMAND ----------
# MAGIC %md
# MAGIC ## 📊 5. Transformación 3: Agregaciones Analíticas OLAP (`groupBy` + `agg`)
# MAGIC Construimos la tabla de hechos resumida para el Data Warehouse y los Dashboards de Business Intelligence.

# COMMAND ----------
df_kpis_genero = (
    df_enriquecido
    .groupBy("genero_individual")
    .agg(
        F.count("evento_id").alias("total_reproducciones"),
        F.round(F.avg("completitud_pct"), 1).alias("completitud_promedio_pct"),
        F.round(F.avg("score_qos"), 1).alias("qos_promedio"),
        F.round(F.sum("minutos_vistos") / 60, 2).alias("horas_totales_streaming"),
        F.sum(F.when(F.col("abandono_prematuro") == True, 1).otherwise(0)).alias("total_abandonos")
    )
    .orderBy(F.col("total_reproducciones").desc(), F.col("horas_totales_streaming").desc())
)

print("🏆 KPIs Analíticos por Género Cinematográfico:")
display(df_kpis_genero)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 💾 6. Exportación Nativa a DBFS y Delta Lake (Sin Parches Locales)
# MAGIC En Databricks la persistencia es 100% nativa de Apache Spark hacia el almacenamiento distribuido.

# COMMAND ----------
# Ruta en el Databricks File System (DBFS)
ruta_dbfs_parquet = "dbfs:/FileStore/hyperflix/curated/kpis_genero_parquet"
ruta_dbfs_delta = "dbfs:/FileStore/hyperflix/curated/kpis_genero_delta"

# 1. Guardar en Parquet estándar
df_kpis_genero.write.mode("overwrite").parquet(ruta_dbfs_parquet)
print("✅ Tabla Parquet guardada exitosamente en:", ruta_dbfs_parquet)

# 2. Guardar en Delta Lake (Estándar Enterprise Lakehouse)
df_kpis_genero.write.format("delta").mode("overwrite").save(ruta_dbfs_delta)
print("✅ Tabla Delta Lake guardada exitosamente en:", ruta_dbfs_delta)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 🔍 7. Verificación de Lectura desde DBFS
# MAGIC Leemos directamente la tabla persistida para confirmar la integridad del almacenamiento columnar.

# COMMAND ----------
df_verificacion = spark.read.format("delta").load(ruta_dbfs_delta)
print(f"🎉 Lectura exitosa desde Delta Lake ({df_verificacion.count()} filas):")
display(df_verificacion)
