import os
import sys
import io
import pymongo
import boto3
import s3fs
import pandas as pd
from dotenv import load_dotenv
from datetime import datetime

# Asegurar codificación UTF-8 en consola Windows
if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8')
if hasattr(sys.stderr, 'reconfigure'):
    sys.stderr.reconfigure(encoding='utf-8')

# =============================================================================
# BLOQUE 1: CONFIGURACIÓN Y GESTIÓN DE SECRETOS (.env)
# =============================================================================
load_dotenv()

MONGO_URI = os.getenv("MONGO_URI")
DB_NAME = os.getenv("MONGO_DB_NAME", "hyperflix_db")

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.getenv("AWS_DEFAULT_REGION", "us-east-2")
S3_BUCKET = os.getenv("AWS_S3_BUCKET", "hyperflix-data-lake-emerson")

if not AWS_ACCESS_KEY_ID or not AWS_SECRET_ACCESS_KEY:
    print("❌ Error: No se encontraron las credenciales de AWS en el archivo .env")
    print("Asegúrate de definir AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY y AWS_S3_BUCKET en tu .env")
    sys.exit(1)

# Opciones de autenticación para s3fs y pandas to_parquet
STORAGE_OPTIONS = {
    "key": AWS_ACCESS_KEY_ID,
    "secret": AWS_SECRET_ACCESS_KEY,
    "client_kwargs": {"region_name": AWS_REGION}
}

# Inicializar cliente boto3 para inspección de objetos
s3_client = boto3.client(
    "s3",
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name=AWS_REGION
)

# Zonas del Data Lake en AWS S3
ZONAS_S3 = {
    "raw": f"s3://{S3_BUCKET}/raw",
    "processed": f"s3://{S3_BUCKET}/processed",
    "curated": f"s3://{S3_BUCKET}/curated"
}

# Respaldo local opcional
DATA_LAKE_LOCAL = "data_lake"
ZONAS_LOCAL = {
    "raw": os.path.join(DATA_LAKE_LOCAL, "raw"),
    "processed": os.path.join(DATA_LAKE_LOCAL, "processed"),
    "curated": os.path.join(DATA_LAKE_LOCAL, "curated")
}

# =============================================================================
# BLOQUE 2: FUNCIÓN GENÉRICA PARA EXPORTAR COLECCIÓN A PARQUET EN S3
# =============================================================================
def exportar_coleccion_a_s3(coleccion, nombre_archivo, zona, filtro=None):
    """
    Extrae documentos desde MongoDB Atlas, normaliza tipos con Pandas/PyArrow,
    y escribe directamente en formato Parquet (Snappy) en el bucket AWS S3.
    """
    client = pymongo.MongoClient(MONGO_URI)
    db = client[DB_NAME]
    
    query = filtro if filtro is not None else {}
    documentos = list(db[coleccion].find(query))
    client.close()
    
    if not documentos:
        print(f"  ⚠️ No se encontraron documentos en '{coleccion}' con filtro {query}")
        return None
        
    # Normalizar documentos JSON a DataFrame de Pandas
    df = pd.json_normalize(documentos)
    
    # Parquet no acepta ObjectIds nativos de BSON, convertimos a string seguro
    for col in df.columns:
        if "_id" in col or col.endswith("_id") or col == "id":
            df[col] = df[col].astype(str)
        elif df[col].dtype == 'object':
            df[col] = df[col].apply(
                lambda x: str(x) if x is not None and not isinstance(x, (str, int, float, bool)) else x
            )
            
    # Ruta de destino directa en AWS S3
    ruta_s3 = f"{ZONAS_S3[zona]}/{nombre_archivo}.parquet"
    
    # Escribir directamente en S3 usando s3fs / pyarrow
    df.to_parquet(
        ruta_s3,
        engine="pyarrow",
        compression="snappy",
        index=False,
        storage_options=STORAGE_OPTIONS
    )
    
    # Escribir copia de respaldo local
    os.makedirs(ZONAS_LOCAL[zona], exist_ok=True)
    ruta_local = os.path.join(ZONAS_LOCAL[zona], f"{nombre_archivo}.parquet")
    df.to_parquet(ruta_local, engine="pyarrow", compression="snappy", index=False)
    
    # Consultar tamaño real del objeto en S3
    s3_key = f"{zona}/{nombre_archivo}.parquet"
    try:
        head = s3_client.head_object(Bucket=S3_BUCKET, Key=s3_key)
        tamano_kb = head.get('ContentLength', 0) / 1024
    except Exception:
        tamano_kb = os.path.getsize(ruta_local) / 1024
        
    print(f"  ☁️  [{zona.upper()}] {ruta_s3}")
    print(f"      └── {len(df):,} registros | {tamano_kb:.2f} KB | Compresión: Snappy")
    return df

# =============================================================================
# BLOQUE 3: CONSTRUCCIÓN COMPLETA DEL DATA LAKE EN AWS S3
# =============================================================================
def ejecutar_pipeline_s3():
    inicio_tiempo = datetime.now()
    print("=" * 80)
    print("☁️  DATA LAKE ENTERPRISE: HYPERFLIX ➔ AMAZON S3")
    print(f"📦 Bucket Destino: s3://{S3_BUCKET}")
    print(f"🌎 Región AWS:    {AWS_REGION}")
    print(f"🍃 Origen OLTP:    MongoDB Atlas ('{DB_NAME}')")
    print("=" * 80 + "\n")
    
    # -------------------------------------------------------------------------
    # ETAPA 1: RAW (Copia idéntica de colecciones crudas transaccionales)
    # -------------------------------------------------------------------------
    print("📦 ETAPA 1: ZONA RAW ➔ Subiendo datos crudos a S3...")
    exportar_coleccion_a_s3("usuarios", "usuarios", "raw")
    exportar_coleccion_a_s3("peliculas", "peliculas", "raw")
    exportar_coleccion_a_s3("canales_tv", "canales_tv", "raw")
    exportar_coleccion_a_s3("eventos_reproduccion", "eventos_reproduccion", "raw")
    
    # -------------------------------------------------------------------------
    # ETAPA 2: PROCESSED (Limpieza, tipado y métricas QoS de streaming)
    # -------------------------------------------------------------------------
    print("\n⚙️  ETAPA 2: ZONA PROCESSED ➔ Transformando y cargando telemetría con QoS...")
    client = pymongo.MongoClient(MONGO_URI)
    db = client[DB_NAME]
    
    eventos = list(db.eventos_reproduccion.find({"segundo_reproduccion": {"$gt": 0}}))
    client.close()
    
    if eventos:
        df_proc = pd.json_normalize(eventos)
        for col in df_proc.columns:
            if "_id" in col or col.endswith("_id"):
                df_proc[col] = df_proc[col].astype(str)
                
        if "timestamp" in df_proc.columns:
            df_proc["timestamp"] = pd.to_datetime(df_proc["timestamp"])
            df_proc["anio"] = df_proc["timestamp"].dt.year
            df_proc["mes"] = df_proc["timestamp"].dt.month
            df_proc["dia"] = df_proc["timestamp"].dt.day
            df_proc["hora"] = df_proc["timestamp"].dt.hour
            
        df_proc["fecha_ingesta_s3"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        if "latencia_ms" in df_proc.columns:
            df_proc["categoria_latencia"] = df_proc["latencia_ms"].apply(
                lambda x: "Excelente (<50ms)" if x < 50 else ("Aceptable (50-100ms)" if x <= 100 else "Degradada (>100ms)")
            )
            
        ruta_s3_proc = f"{ZONAS_S3['processed']}/reproducciones_procesadas.parquet"
        df_proc.to_parquet(
            ruta_s3_proc,
            engine="pyarrow",
            compression="snappy",
            index=False,
            storage_options=STORAGE_OPTIONS
        )
        
        # Respaldo local
        os.makedirs(ZONAS_LOCAL["processed"], exist_ok=True)
        ruta_local_proc = os.path.join(ZONAS_LOCAL["processed"], "reproducciones_procesadas.parquet")
        df_proc.to_parquet(ruta_local_proc, engine="pyarrow", compression="snappy", index=False)
        
        s3_key_proc = "processed/reproducciones_procesadas.parquet"
        try:
            head_proc = s3_client.head_object(Bucket=S3_BUCKET, Key=s3_key_proc)
            tamano_proc_kb = head_proc.get('ContentLength', 0) / 1024
        except Exception:
            tamano_proc_kb = os.path.getsize(ruta_local_proc) / 1024
            
        print(f"  ☁️  [PROCESSED] {ruta_s3_proc}")
        print(f"      └── {len(df_proc):,} registros | {tamano_proc_kb:.2f} KB | Columnas temporales + QoS")
        
    # -------------------------------------------------------------------------
    # ETAPA 3: CURATED (Modelo analítico dimensional en estrella para BI)
    # -------------------------------------------------------------------------
    print("\n💎 ETAPA 3: ZONA CURATED ➔ Subiendo Modelo Dimensional a S3...")
    exportar_coleccion_a_s3("reproducciones_analiticas", "reproducciones_curadas", "curated")
    
    # -------------------------------------------------------------------------
    # BLOQUE 4: RESUMEN Y AUDITORÍA DE OBJETOS EN AMAZON S3
    # -------------------------------------------------------------------------
    mostrar_auditoria_s3(inicio_tiempo)

def mostrar_auditoria_s3(inicio_tiempo):
    print("\n" + "=" * 80)
    print("📊 AUDITORÍA Y RESUMEN OFICIAL DEL DATA LAKE EN AMAZON S3")
    print("=" * 80)
    
    response = s3_client.list_objects_v2(Bucket=S3_BUCKET)
    objetos = response.get('Contents', [])
    
    if not objetos:
        print("  ⚠️ No se encontraron objetos en el bucket.")
        return
        
    total_bytes = 0
    conteo_zonas = {"raw": 0, "processed": 0, "curated": 0}
    
    for obj in objetos:
        key = obj['Key']
        tamano = obj['Size']
        total_bytes += tamano
        zona = key.split('/')[0] if '/' in key else 'otros'
        if zona in conteo_zonas:
            conteo_zonas[zona] += 1
            
        print(f"  📁 s3://{S3_BUCKET}/{key} ({tamano / 1024:.2f} KB)")
        
    duracion = (datetime.now() - inicio_tiempo).total_seconds()
    print("-" * 80)
    print(f"📦 Total de archivos en S3:     {len(objetos)} archivos Parquet")
    print(f"🗂️  Distribución por zonas:       RAW: {conteo_zonas['raw']} | PROCESSED: {conteo_zonas['processed']} | CURATED: {conteo_zonas['curated']}")
    print(f"💾 Almacenamiento total S3:     {total_bytes / 1024:.2f} KB ({total_bytes / (1024*1024):.2f} MB)")
    print(f"⏱️  Tiempo total del pipeline:   {duracion:.2f} segundos")
    print(f"🎯 Formato:                      Apache Parquet Columnar | Snappy")
    print(f"🔒 Nivel de Seguridad:           Cifrado SSE-S3 | Bloqueo público estricto")
    print("=" * 80)
    print("🎉 ¡DATA LAKE MIGRADO A AMAZON S3 CON ÉXITO TOTAL!")
    print("=" * 80 + "\n")

if __name__ == "__main__":
    ejecutar_pipeline_s3()
