"""
============================================================
  HYPERFLIX — MongoDB Atlas → Google BigQuery
  Data Warehouse Loader
============================================================
  Proyecto : hyperflix-analytics
  Dataset  : hyperflix_dw
  Tablas   : dim_contenido, dim_usuario, dim_dispositivo,
             dim_fecha, fact_reproducciones
============================================================
"""

import os
import sys
import hashlib
import random
from datetime import datetime, timedelta, date

# Configurar encoding seguro para Windows
if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8')


import pymongo
import pandas as pd
from dotenv import load_dotenv
from google.cloud import bigquery
from google.oauth2 import service_account

# ─────────────────────────────────────────────
# CONFIGURACIÓN
# ─────────────────────────────────────────────
load_dotenv()

MONGO_URI   = os.getenv("MONGO_URI")
MONGO_DB    = os.getenv("MONGO_DB_NAME", "hyperflix_db")
GCP_PROJECT = "hyperflix-analytics"
BQ_DATASET  = "hyperflix_dw"

# Credenciales BigQuery: usa la variable de entorno GOOGLE_APPLICATION_CREDENTIALS
# o el archivo de clave JSON que descargaste
KEY_PATH = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "hyperflix-bq-key.json")

# ─────────────────────────────────────────────
# CONEXIONES
# ─────────────────────────────────────────────
print("🔌 Conectando a MongoDB Atlas...")
mongo_client = pymongo.MongoClient(MONGO_URI)
db = mongo_client[MONGO_DB]
print(f"   ✅ Conectado a: {MONGO_DB}")

print("🔌 Conectando a Google BigQuery...")
try:
    if os.path.exists(KEY_PATH):
        creds = service_account.Credentials.from_service_account_file(KEY_PATH)
        bq_client = bigquery.Client(project=GCP_PROJECT, credentials=creds)
    else:
        # Application Default Credentials (gcloud auth application-default login)
        bq_client = bigquery.Client(project=GCP_PROJECT)
    print(f"   ✅ Conectado al proyecto: {GCP_PROJECT}")
except Exception as e:
    print(f"   ❌ Error BigQuery: {e}")
    raise

TABLE = lambda name: f"{GCP_PROJECT}.{BQ_DATASET}.{name}"

# ─────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────
def uid(val: str) -> str:
    """Genera un ID corto reproducible a partir de un valor."""
    return hashlib.md5(str(val).encode()).hexdigest()[:12]

def load_table(table_id: str, df: pd.DataFrame):
    """Sube un DataFrame a BigQuery (reemplaza la tabla)."""
    if df.empty:
        print(f"   ⚠️  DataFrame vacío para {table_id}, se omite.")
        return
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
    job = bq_client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()
    print(f"   ✅ {table_id.split('.')[-1]}: {len(df):,} filas cargadas.")

# ─────────────────────────────────────────────
# 1. DIM_CONTENIDO — desde colección 'peliculas'
# ─────────────────────────────────────────────
print("\n📦 [1/5] Cargando dim_contenido...")
peliculas = list(db["peliculas"].find({}, {"_id": 0}))
print(f"   🔍 {len(peliculas)} películas encontradas en MongoDB")

rows_contenido = []
for p in peliculas:
    generos = p.get("generos", p.get("genero", "Desconocido"))
    if isinstance(generos, list):
        genero_str = ", ".join(generos)
        genero_prim = generos[0] if generos else "Desconocido"
    else:
        genero_str = str(generos)
        genero_prim = str(generos).split(",")[0].strip()

    rows_contenido.append({
        "contenido_id"    : uid(p.get("titulo", str(p))),
        "titulo"          : str(p.get("titulo", "Sin título")),
        "genero_primario" : genero_prim,
        "generos"         : genero_str,
        "anio_estreno"    : int(p.get("anio_estreno", p.get("year", 2020))),
        "duracion_min"    : float(p.get("duracion_min", p.get("duracion", 90))),
        "rating_promedio" : float(p.get("rating_promedio", p.get("rating", 7.0))),
        "idioma"          : str(p.get("idioma", "Español")),
        "fecha_carga"     : datetime.utcnow(),
    })

df_contenido = pd.DataFrame(rows_contenido).drop_duplicates("contenido_id")
load_table(TABLE("dim_contenido"), df_contenido)

# Índice título → contenido_id para la tabla de hechos
titulo_to_id = {r["titulo"]: r["contenido_id"] for r in rows_contenido}
genero_map   = {r["contenido_id"]: r["genero_primario"] for r in rows_contenido}

# ─────────────────────────────────────────────
# 2. DIM_DISPOSITIVO — valores canónicos
# ─────────────────────────────────────────────
print("\n📦 [2/5] Cargando dim_dispositivo...")
dispositivos = [
    {"dispositivo_id": "SMART_TV",  "tipo_dispositivo": "Smart TV",  "categoria": "TV"},
    {"dispositivo_id": "MOVIL",     "tipo_dispositivo": "Móvil",      "categoria": "Mobile"},
    {"dispositivo_id": "TABLET",    "tipo_dispositivo": "Tablet",     "categoria": "Mobile"},
    {"dispositivo_id": "PC",        "tipo_dispositivo": "PC",         "categoria": "Desktop"},
    {"dispositivo_id": "CONSOLA",   "tipo_dispositivo": "Consola",    "categoria": "Gaming"},
    {"dispositivo_id": "DESCONOCIDO","tipo_dispositivo":"Desconocido","categoria": "Otro"},
]
df_disp = pd.DataFrame(dispositivos)
load_table(TABLE("dim_dispositivo"), df_disp)

disp_norm = {
    "smart tv": "SMART_TV", "smarttv": "SMART_TV", "tv": "SMART_TV",
    "móvil": "MOVIL", "movil": "MOVIL", "mobile": "MOVIL", "smartphone": "MOVIL",
    "tablet": "TABLET",
    "pc": "PC", "laptop": "PC", "computador": "PC",
    "consola": "CONSOLA", "playstation": "CONSOLA", "xbox": "CONSOLA",
}

# ─────────────────────────────────────────────
# 3. Leer eventos reales desde MongoDB
# ─────────────────────────────────────────────
print("\n🔍 Leyendo eventos de reproducciones desde MongoDB...")
# Intentar desde reproducciones_analiticas primero
coleccion = "reproducciones_analiticas"
eventos_raw = list(db[coleccion].find({}, {"_id": 0}))
if not eventos_raw:
    coleccion = "eventos_reproduccion"
    eventos_raw = list(db[coleccion].find({}, {"_id": 0}))
print(f"   🔍 {len(eventos_raw):,} eventos encontrados en '{coleccion}'")

# ─────────────────────────────────────────────
# 4. DIM_USUARIO — extraído de los eventos
# ─────────────────────────────────────────────
print("\n📦 [3/5] Cargando dim_usuario...")
usuarios_vistos = {}
for ev in eventos_raw:
    uid_val = str(ev.get("usuario_id", ev.get("user_id", "USR_" + str(random.randint(1000,9999)))))
    if uid_val not in usuarios_vistos:
        usuarios_vistos[uid_val] = {
            "usuario_id" : uid_val,
            "pais"       : str(ev.get("pais", ev.get("country", "Colombia"))),
            "plan"       : str(ev.get("plan", ev.get("subscription", "Premium"))),
            "dispositivo": str(ev.get("dispositivo", ev.get("device", "Móvil"))),
            "fecha_carga": datetime.utcnow(),
        }

df_usuario = pd.DataFrame(list(usuarios_vistos.values()))
load_table(TABLE("dim_usuario"), df_usuario)

# ─────────────────────────────────────────────
# 5. DIM_FECHA — rango de fechas del dataset
# ─────────────────────────────────────────────
print("\n📦 [4/5] Cargando dim_fecha...")
# Generar fechas desde 2024-01-01 hasta hoy
fechas = []
start = date(2024, 1, 1)
end   = date.today()
current = start
meses_es = ["","Enero","Febrero","Marzo","Abril","Mayo","Junio",
            "Julio","Agosto","Septiembre","Octubre","Noviembre","Diciembre"]
dias_es  = ["Lunes","Martes","Miércoles","Jueves","Viernes","Sábado","Domingo"]

while current <= end:
    fechas.append({
        "fecha_id"  : current,
        "anio"      : current.year,
        "mes"       : current.month,
        "dia"       : current.day,
        "trimestre" : (current.month - 1) // 3 + 1,
        "nombre_mes": meses_es[current.month],
        "dia_semana": dias_es[current.weekday()],
    })
    current += timedelta(days=1)

df_fecha = pd.DataFrame(fechas)
df_fecha["fecha_id"] = pd.to_datetime(df_fecha["fecha_id"]).dt.date
load_table(TABLE("dim_fecha"), df_fecha)

# ─────────────────────────────────────────────
# 6. FACT_REPRODUCCIONES
# ─────────────────────────────────────────────
print("\n📦 [5/5] Cargando fact_reproducciones...")

rows_fact = []
for i, ev in enumerate(eventos_raw):
    # Título / contenido_id
    titulo = str(ev.get("titulo", ev.get("title", ev.get("pelicula", ""))))
    cid = titulo_to_id.get(titulo)
    if not cid:
        cid = uid(titulo) if titulo else f"CNT_{i:06d}"

    # Usuario
    user_id = str(ev.get("usuario_id", ev.get("user_id", f"USR_{i:06d}")))

    # Dispositivo
    raw_disp = str(ev.get("dispositivo", ev.get("device", "desconocido"))).lower()
    disp_id  = disp_norm.get(raw_disp, "DESCONOCIDO")

    # Métricas numéricas
    minutos  = float(ev.get("minutos_vistos", ev.get("minutes_watched", random.uniform(20, 120))))
    duracion = float(ev.get("duracion_total_min", ev.get("duration_min", 90)))
    if duracion == 0:
        duracion = 90.0
    completitud = round(min((minutos / duracion) * 100, 100), 2)

    latencia     = int(ev.get("latencia_ms", ev.get("latency_ms", random.randint(50, 500))))
    buffering    = int(ev.get("buffering_count", ev.get("buffering", random.randint(0, 5))))
    abandono     = bool(ev.get("abandono_prematuro", completitud < 30))
    score_qos    = round(ev.get("score_qos", max(0, 10 - latencia/200 - buffering*0.5)), 2)

    # Fecha
    fecha_raw = ev.get("fecha_evento", ev.get("timestamp", ev.get("fecha", None)))
    if fecha_raw and isinstance(fecha_raw, datetime):
        fecha_ts = fecha_raw
    elif fecha_raw:
        try:
            fecha_ts = pd.to_datetime(fecha_raw)
        except:
            fecha_ts = datetime.utcnow() - timedelta(days=random.randint(0, 365))
    else:
        fecha_ts = datetime.utcnow() - timedelta(days=random.randint(0, 365))

    fecha_id = fecha_ts.date() if hasattr(fecha_ts, "date") else date.today()

    rows_fact.append({
        "evento_id"          : str(ev.get("evento_id", ev.get("_id", f"EVT_{i:08d}"))),
        "contenido_id"       : cid,
        "usuario_id"         : user_id,
        "dispositivo_id"     : disp_id,
        "fecha_id"           : fecha_id,
        "minutos_vistos"     : minutos,
        "duracion_total_min" : duracion,
        "completitud_pct"    : completitud,
        "latencia_ms"        : latencia,
        "buffering_count"    : buffering,
        "abandono_prematuro" : abandono,
        "score_qos"          : score_qos,
        "fecha_evento"       : fecha_ts,
    })

df_fact = pd.DataFrame(rows_fact)
df_fact["fecha_id"]    = pd.to_datetime(df_fact["fecha_id"]).dt.date
df_fact["fecha_evento"] = pd.to_datetime(df_fact["fecha_evento"], utc=True)
df_fact["latencia_ms"]  = df_fact["latencia_ms"].astype("int64")
df_fact["buffering_count"] = df_fact["buffering_count"].astype("int64")

load_table(TABLE("fact_reproducciones"), df_fact)

# ─────────────────────────────────────────────
# RESUMEN FINAL
# ─────────────────────────────────────────────
print("\n" + "="*55)
print("  ✅ HYPERFLIX DATA WAREHOUSE — CARGA COMPLETADA")
print("="*55)
print(f"  dim_contenido      : {len(df_contenido):>8,} filas")
print(f"  dim_usuario        : {len(df_usuario):>8,} filas")
print(f"  dim_dispositivo    : {len(df_disp):>8,} filas")
print(f"  dim_fecha          : {len(df_fecha):>8,} filas")
print(f"  fact_reproducciones: {len(df_fact):>8,} filas")
print("="*55)
print(f"\n🔗 BigQuery Console:")
print(f"   https://console.cloud.google.com/bigquery?project={GCP_PROJECT}")
