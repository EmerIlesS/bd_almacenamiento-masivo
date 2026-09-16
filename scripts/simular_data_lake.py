import os
import sys
import json
from datetime import datetime
import pymongo
from dotenv import load_dotenv

# Configurar salida UTF-8 para consola Windows
if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8')
if hasattr(sys.stderr, 'reconfigure'):
    sys.stderr.reconfigure(encoding='utf-8')

load_dotenv()
MONGO_URI = os.getenv("MONGO_URI")
DB_NAME = os.getenv("MONGO_DB_NAME", "hyperflix_db")

def simular_data_lake_hyperflix():
    print("=" * 75)
    print("🌊 HYPERFLIX: SIMULADOR DE ZONAS DEL DATA LAKE (MINIO / S3)")
    print("Arquitectura de 4 Capas: RAW ➔ BRONZE ➔ SILVER ➔ GOLD")
    print("=" * 75 + "\n")
    
    base_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data_lake")
    
    # 1. Crear directorios de las 4 zonas del Data Lake
    zonas = {
        "raw": os.path.join(base_dir, "raw", "telemetria_video", "year=2026", "month=09", "day=02"),
        "raw_logs": os.path.join(base_dir, "raw", "logs_sistema", "year=2026", "month=09", "day=02"),
        "bronze": os.path.join(base_dir, "bronze", "interacciones", "year=2026", "month=09", "day=02"),
        "silver": os.path.join(base_dir, "silver", "reproducciones_validadas", "year=2026", "month=09", "day=02"),
        "gold_ranking": os.path.join(base_dir, "gold", "metricas_diarias_contenido", "year=2026", "month=09", "day=02"),
        "gold_retencion": os.path.join(base_dir, "gold", "retencion_usuarios")
    }
    
    for k, ruta in zonas.items():
        os.makedirs(ruta, exist_ok=True)
        print(f"📁 Zona asegurada: {os.path.relpath(ruta, base_dir)}")
        
    client = pymongo.MongoClient(MONGO_URI)
    db = client[DB_NAME]
    
    # 2. CAPA 1: RAW (Dumps originales directos desde Kafka en JSON puro)
    print("\n📦 1/4. Extrayendo y particionando Capa RAW...")
    eventos_crudos = list(db.eventos_reproduccion.find({}, {"_id": 0}).limit(500))
    if not eventos_crudos:
        print("⚠️ No se encontraron eventos en BD. Insertando muestra sintética en RAW...")
        eventos_crudos = [{"mock_event": "PLAY", "timestamp": datetime.now().isoformat()}]
        
    ruta_raw = os.path.join(zonas["raw"], "eventos_raw.json")
    with open(ruta_raw, "w", encoding="utf-8") as f:
        json.dump(eventos_crudos, f, default=str, indent=2, ensure_ascii=False)
    print(f"  ✅ RAW guardado: {len(eventos_crudos)} eventos crudos en '{os.path.relpath(ruta_raw, base_dir)}'")
    
    # 3. CAPA 2: BRONZE (Datos estructurados, tipados y particionados por origen y fecha)
    print("\n🥉 2/4. Procesando Capa BRONZE (Estructurada y Particionada)...")
    eventos_bronze = []
    for ev in eventos_crudos:
        eventos_bronze.append({
            "schema_version": "2.1.0",
            "ingestion_engine": "Apache Kafka Connect",
            "event_id": ev.get("codigo_evento", "EVT-UNKNOWN"),
            "user_ref": str(ev.get("usuario_id", "")),
            "content_ref": str(ev.get("contenido_id", "")),
            "action": ev.get("tipo_evento", "HEARTBEAT"),
            "device_model": ev.get("dispositivo", "Desconocido"),
            "event_time_utc": str(ev.get("timestamp", datetime.now().isoformat())),
            "payload_telemetry": {
                "watch_seconds": ev.get("segundo_reproduccion", 0),
                "bitrate": ev.get("bitrate_kbps", 0),
                "latency_ms": ev.get("latencia_ms", 0)
            }
        })
        
    ruta_bronze = os.path.join(zonas["bronze"], "part-001.json")
    with open(ruta_bronze, "w", encoding="utf-8") as f:
        json.dump(eventos_bronze, f, default=str, indent=2, ensure_ascii=False)
    print(f"  ✅ BRONZE guardado: {len(eventos_bronze)} registros en '{os.path.relpath(ruta_bronze, base_dir)}'")
    
    # 4. CAPA 3: SILVER (Datos limpios, validados y desnormalizados para Spark ML)
    print("\n🥈 3/4. Procesando Capa SILVER (Limpia y Enriquecida para Recomendaciones ML)...")
    datos_silver = list(db.reproducciones_analiticas.find({}, {"_id": 0}).limit(500))
    if not datos_silver:
        datos_silver = eventos_bronze
        
    ruta_silver = os.path.join(zonas["silver"], "reproducciones_validadas.json")
    with open(ruta_silver, "w", encoding="utf-8") as f:
        json.dump(datos_silver, f, default=str, indent=2, ensure_ascii=False)
    print(f"  ✅ SILVER guardado: {len(datos_silver)} registros en '{os.path.relpath(ruta_silver, base_dir)}'")
    
    # 5. CAPA 4: GOLD (Agregaciones de alto nivel para BI y ClickHouse)
    print("\n🥇 4/4. Generando Capa GOLD (Métricas y Rankings de Negocio)...")
    
    # Ranking de películas
    pipeline_top = [
        {"$match": {"contenido.tipo": "Pelicula"}},
        {
            "$group": {
                "_id": "$contenido.titulo",
                "genero": {"$first": "$contenido.genero_o_categoria"},
                "vistas": {"$sum": 1},
                "horas_totales": {"$sum": {"$divide": ["$duracion_vista_minutos", 60]}}
            }
        },
        {"$sort": {"horas_totales": -1}},
        {"$limit": 10}
    ]
    top_peliculas = list(db.reproducciones_analiticas.aggregate(pipeline_top))
    
    ruta_gold_top = os.path.join(zonas["gold_ranking"], "top_peliculas.json")
    with open(ruta_gold_top, "w", encoding="utf-8") as f:
        json.dump(top_peliculas, f, default=str, indent=2, ensure_ascii=False)
    print(f"  ✅ GOLD Top Películas guardado en '{os.path.relpath(ruta_gold_top, base_dir)}'")
    
    # Reporte de retención y calidad
    reporte_kpis = {
        "generado_el": datetime.now().isoformat(),
        "plataforma": "HYPERFLIX Global Streaming",
        "total_usuarios_registrados": db.usuarios.count_documents({}),
        "suscripciones_activas": db.suscripciones.count_documents({"estado": "Activa"}),
        "catalogo_peliculas": db.peliculas.count_documents({}),
        "canales_iptv_libres": db.canales_tv.count_documents({}),
        "reproducciones_analizadas": db.reproducciones_analiticas.count_documents({})
    }
    ruta_gold_retencion = os.path.join(zonas["gold_retencion"], "reporte_kpis.json")
    with open(ruta_gold_retencion, "w", encoding="utf-8") as f:
        json.dump(reporte_kpis, f, default=str, indent=2, ensure_ascii=False)
    print(f"  ✅ GOLD Reporte KPIs guardado en '{os.path.relpath(ruta_gold_retencion, base_dir)}'")
    
    print("\n" + "=" * 75)
    print("🎉 ¡SIMULACIÓN DEL DATA LAKE HYPERFLIX COMPLETADA CON ÉXITO!")
    print("=" * 75)
    
    client.close()

if __name__ == "__main__":
    simular_data_lake_hyperflix()
