import os
import sys
import time
import pandas as pd

# Asegurar codificación UTF-8 en consola Windows
if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8')
if hasattr(sys.stderr, 'reconfigure'):
    sys.stderr.reconfigure(encoding='utf-8')

def consultar_data_lake_parquet():
    print("=" * 75)
    print("🔍 CONSULTA Y ANÁLISIS DE ARCHIVOS PARQUET EN EL DATA LAKE (PANDAS)")
    print("=" * 75 + "\n")
    
    ruta_curated = os.path.join("data_lake", "curated", "reproducciones_curadas.parquet")
    ruta_processed = os.path.join("data_lake", "processed", "reproducciones_procesadas.parquet")
    
    if not os.path.exists(ruta_curated):
        print(f"⚠️ No se encontró el archivo '{ruta_curated}'. Ejecuta primero: python data_lake.py")
        return

    # 1. Lectura de Parquet con compresión Snappy
    print(f"📖 1. Leyendo zona CURATED: '{ruta_curated}'...")
    t0 = time.perf_counter()
    df_curated = pd.read_parquet(ruta_curated, engine="pyarrow")
    t_lectura = (time.perf_counter() - t0) * 1000
    
    print(f"   ⏱️ Tiempo de lectura de {len(df_curated):,} filas Parquet: {t_lectura:.2f} ms")
    print(f"   💾 Uso de memoria del DataFrame: {df_curated.memory_usage().sum() / 1024:.2f} KB\n")
    
    # 2. Resumen de Columnas
    print("📋 Columnas disponibles en el Modelo Dimensional Curated:")
    print("   " + ", ".join(list(df_curated.columns[:10])) + ", ...\n")
    
    # 3. Consulta Analítica 1: Top 5 Películas más vistas en Curated
    print("-" * 75)
    print("📊 CONSULTA 1: Top 5 Películas con Mayor Tiempo Reproducido (Minutos)")
    print("-" * 75)
    if "contenido.titulo" in df_curated.columns and "duracion_vista_minutos" in df_curated.columns:
        # Asegurar tipo numérico
        df_curated["duracion_vista_minutos"] = pd.to_numeric(df_curated["duracion_vista_minutos"], errors="coerce")
        top_pelis = (
            df_curated.groupby("contenido.titulo")["duracion_vista_minutos"]
            .agg(minutos_totales="sum", reproducciones="count")
            .sort_values(by="minutos_totales", ascending=False)
            .head(5)
            .reset_index()
        )
        top_pelis["horas_totales"] = (top_pelis["minutos_totales"] / 60).round(1)
        print(top_pelis.to_string(index=False))
    print()

    # 4. Consulta Analítica 2: Calidad de Servicio (QoS) por Dispositivo
    print("-" * 75)
    print("📊 CONSULTA 2: Métricas de Calidad de Servicio (QoS) por Tipo de Dispositivo")
    print("-" * 75)
    if "dispositivo.tipo" in df_curated.columns and "latencia_ms" in df_curated.columns:
        df_curated["latencia_ms"] = pd.to_numeric(df_curated["latencia_ms"], errors="coerce")
        df_curated["bitrate_kbps"] = pd.to_numeric(df_curated["bitrate_kbps"], errors="coerce")
        
        qos_disp = (
            df_curated.groupby("dispositivo.tipo")
            .agg(
                reproducciones=("latencia_ms", "count"),
                latencia_promedio_ms=("latencia_ms", "mean"),
                bitrate_promedio_kbps=("bitrate_kbps", "mean")
            )
            .round(1)
            .reset_index()
        )
        print(qos_disp.to_string(index=False))
    print()

    # 5. Consulta Analítica 3: Lectura rápida de zona PROCESSED
    print("-" * 75)
    print("📊 CONSULTA 3: Lectura y Distribución de Latencia en Zona PROCESSED")
    print("-" * 75)
    if os.path.exists(ruta_processed):
        df_proc = pd.read_parquet(ruta_processed, engine="pyarrow")
        if "categoria_latencia" in df_proc.columns:
            distribucion = df_proc["categoria_latencia"].value_counts().reset_index()
            distribucion.columns = ["Categoría de Latencia", "Total Eventos"]
            print(distribucion.to_string(index=False))
            
    print("\n" + "=" * 75)
    print("🎉 ¡CONSULTAS ANALÍTICAS SOBRE PARQUET EJECUTADAS CON ÉXITO!")
    print("🎯 Ventaja Parquet: Lectura columnar ultrarrápida sin necesidad de consultar la base de datos.")
    print("=" * 75)

if __name__ == "__main__":
    consultar_data_lake_parquet()
