import os
import sys
import pymongo
import pandas as pd
from dotenv import load_dotenv
from datetime import datetime

# Asegurar codificación UTF-8 en consola Windows
if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8')
if hasattr(sys.stderr, 'reconfigure'):
    sys.stderr.reconfigure(encoding='utf-8')

# Bloque 1. Cargar variables de entorno (Gestión de secretos)
load_dotenv()
MONGO_URI = os.getenv("MONGO_URI")
DB_NAME = os.getenv("MONGO_DB_NAME", "hyperflix_db")

# Bloque 2. Definir el Data Lake y sus tres zonas principales
DATA_LAKE_ROOT = "data_lake"
ZONAS = {
    "raw": os.path.join(DATA_LAKE_ROOT, "raw"),
    "processed": os.path.join(DATA_LAKE_ROOT, "processed"),
    "curated": os.path.join(DATA_LAKE_ROOT, "curated")
}

# Bloque 3. Crear la estructura de carpetas
def crear_estructura_lake():
    print("=" * 70)
    print("🏗️ BLOQUE 3: CREANDO ESTRUCTURA DE ZONAS EN EL DATA LAKE")
    print("=" * 70)
    for nombre, ruta in ZONAS.items():
        os.makedirs(ruta, exist_ok=True)
        print(f"  📁 Zona '{nombre}' lista en: {ruta}")
    print("✅ Estructura del Data Lake inicializada correctamente.\n")

# Bloque 4. Exportar una colección de MongoDB Atlas a formato Parquet
def exportar_a_parquet(coleccion, nombre_archivo, zona, filtro=None):
    """
    Extrae documentos desde MongoDB Atlas, los normaliza con Pandas,
    convierte tipos incompatibles (ObjectId) y los guarda en Parquet con Snappy.
    """
    client = pymongo.MongoClient(MONGO_URI)
    db = client[DB_NAME]
    
    query = filtro if filtro is not None else {}
    documentos = list(db[coleccion].find(query))
    
    if not documentos:
        print(f"  ⚠️ No se encontraron documentos en '{coleccion}' con filtro {query}")
        client.close()
        return None
        
    # Normalizar documentos JSON a DataFrame de Pandas
    df = pd.json_normalize(documentos)
    
    # Parquet no entiende el tipo nativo ObjectId de BSON.
    # Convertimos '_id' y cualquier columna con ObjectId a string.
    for col in df.columns:
        if "_id" in col or col.endswith("_id") or col == "id":
            df[col] = df[col].astype(str)
        elif df[col].dtype == 'object':
            # Si contiene objetos complejos o listas, convertirlos a representación string segura
            df[col] = df[col].apply(lambda x: str(x) if x is not None and not isinstance(x, (str, int, float, bool)) else x)
            
    # Bloque 5. Guardar en Parquet con compresión Snappy
    ruta_salida = os.path.join(ZONAS[zona], f"{nombre_archivo}.parquet")
    df.to_parquet(ruta_salida, engine="pyarrow", compression="snappy", index=False)
    
    tamano_kb = os.path.getsize(ruta_salida) / 1024
    print(f"  ✅ [{zona.upper()}] {nombre_archivo}.parquet | {len(df):,} registros | {tamano_kb:.2f} KB")
    
    client.close()
    return df

# Bloque 6. Construcción completa del Data Lake (Pipeline ETL)
def generar_data_lake_completo():
    print("=" * 70)
    print("🚀 PIPELINE DE DATA LAKE: HYPERFLIX (BIG DATA)")
    print(f"📦 Origen: MongoDB Atlas ('{DB_NAME}') ➔ Python + Pandas ➔ Parquet (Snappy)")
    print("=" * 70 + "\n")
    
    # 1. Crear directorios
    crear_estructura_lake()
    
    # -------------------------------------------------------------
    # ETAPA 1: RAW (Copia exacta de las colecciones sin alterar datos)
    # -------------------------------------------------------------
    print("📦 ETAPA RAW: Extrayendo colecciones completas desde MongoDB Atlas...")
    print("   (Es una copia exacta, sin modificar nada)")
    
    exportar_a_parquet("usuarios", "usuarios", "raw")
    exportar_a_parquet("peliculas", "peliculas", "raw")
    exportar_a_parquet("canales_tv", "canales_tv", "raw")
    exportar_a_parquet("eventos_reproduccion", "eventos_reproduccion", "raw")
    
    # -------------------------------------------------------------
    # ETAPA 2: PROCESSED (Limpieza, filtrado de calidad y enriquecimiento)
    # -------------------------------------------------------------
    print("\n⚙️ ETAPA PROCESSED: Limpieza y transformaciones con Pandas...")
    print("   (Filtrado de eventos válidos y generación de columnas temporales)")
    
    client = pymongo.MongoClient(MONGO_URI)
    db = client[DB_NAME]
    
    # En HyperFlix: filtramos reproducciones con actividad real (segundo_reproduccion > 0)
    eventos = list(db.eventos_reproduccion.find({"segundo_reproduccion": {"$gt": 0}}))
    client.close()
    
    if eventos:
        df_proc = pd.json_normalize(eventos)
        
        # Convertir ObjectIds a string
        for col in df_proc.columns:
            if "_id" in col or col.endswith("_id"):
                df_proc[col] = df_proc[col].astype(str)
                
        # Transformación de Fechas (simula el trabajo de Apache Spark)
        # Asegurar tipo datetime para extraer anio, mes y dia
        if "timestamp" in df_proc.columns:
            df_proc["timestamp"] = pd.to_datetime(df_proc["timestamp"])
            df_proc["anio"] = df_proc["timestamp"].dt.year
            df_proc["mes"] = df_proc["timestamp"].dt.month
            df_proc["dia"] = df_proc["timestamp"].dt.day
            df_proc["hora"] = df_proc["timestamp"].dt.hour
            
        # Agregar metadato de auditoría del pipeline
        df_proc["fecha_procesamiento"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        # Enriquecimiento de Calidad de Servicio (QoS)
        if "latencia_ms" in df_proc.columns:
            df_proc["categoria_latencia"] = df_proc["latencia_ms"].apply(
                lambda x: "Excelente (<50ms)" if x < 50 else ("Aceptable (50-100ms)" if x <= 100 else "Degradada (>100ms)")
            )
            
        # Guardar en zona PROCESSED
        ruta_processed = os.path.join(ZONAS["processed"], "reproducciones_procesadas.parquet")
        df_proc.to_parquet(ruta_processed, engine="pyarrow", compression="snappy", index=False)
        tamano_proc_kb = os.path.getsize(ruta_processed) / 1024
        print(f"  ✅ [PROCESSED] reproducciones_procesadas.parquet | {len(df_proc):,} registros | {tamano_proc_kb:.2f} KB")
        print(f"     💡 Columnas agregadas: anio, mes, dia, hora, fecha_procesamiento, categoria_latencia")
        
    # -------------------------------------------------------------
    # ETAPA 3: CURATED (Modelo en Estrella / Tabla analítica lista para BI)
    # -------------------------------------------------------------
    print("\n💎 ETAPA CURATED: Exportando Modelo Dimensional para Analítica y BI...")
    print("   (FACT_REPRODUCCIONES + Dimensiones de Usuario, Contenido, Dispositivo y Tiempo)")
    
    exportar_a_parquet("reproducciones_analiticas", "reproducciones_curadas", "curated")
    
    # -------------------------------------------------------------
    # BLOQUE 7: Resumen final y estadísticas de almacenamiento
    # -------------------------------------------------------------
    mostrar_resumen_lake()

def mostrar_resumen_lake():
    print("\n" + "=" * 70)
    print("📊 BLOQUE 7: RESUMEN Y ESTADÍSTICAS DEL DATA LAKE")
    print("=" * 70)
    
    total_archivos = 0
    total_tamano_bytes = 0
    
    for zona, ruta in ZONAS.items():
        archivos = [f for f in os.listdir(ruta) if f.endswith(".parquet")]
        tamano_zona = sum(os.path.getsize(os.path.join(ruta, f)) for f in archivos)
        
        total_archivos += len(archivos)
        total_tamano_bytes += tamano_zona
        
        print(f"📁 {zona.upper()}: {len(archivos)} archivo(s) | {tamano_zona / 1024:.2f} KB")
        for arc in archivos:
            peso_arc = os.path.getsize(os.path.join(ruta, arc)) / 1024
            print(f"   └── {arc} ({peso_arc:.2f} KB)")
            
    print("-" * 70)
    print(f"📦 TOTAL DE ARCHIVOS EN EL DATA LAKE: {total_archivos} archivos")
    print(f"💾 ESPACIO TOTAL OCUPADO (SNAPPY): {total_tamano_bytes / 1024:.2f} KB ({total_tamano_bytes / (1024*1024):.2f} MB)")
    print("🎯 Formato: Apache Parquet Columnar | Compresión: Snappy")
    print("=" * 70 + "\n")

# Punto de entrada del programa
if __name__ == "__main__":
    generar_data_lake_completo()
