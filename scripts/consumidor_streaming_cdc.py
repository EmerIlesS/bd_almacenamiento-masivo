import os
import sys
import time
from datetime import datetime
import pymongo
from dotenv import load_dotenv

# Asegurar codificación UTF-8 en consola Windows
if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8')
if hasattr(sys.stderr, 'reconfigure'):
    sys.stderr.reconfigure(encoding='utf-8')

load_dotenv()
MONGO_URI = os.getenv("MONGO_URI")
DB_NAME = os.getenv("MONGO_DB_NAME", "hyperflix_db")

def iniciar_consumidor_streaming():
    print("=" * 78, flush=True)
    print("⚡ HYPERFLIX: CONSUMIDOR DE EVENT STREAMING & CDC (CHANGE DATA CAPTURE)", flush=True)
    print("🎯 Simulación Arquitectónica: Apache Kafka Broker + Spark Streaming Engine", flush=True)
    print(f"📡 Origen CDC: MongoDB Atlas ('{DB_NAME}.eventos_reproduccion') via Change Streams", flush=True)
    print("=" * 78 + "\n", flush=True)

    client = pymongo.MongoClient(MONGO_URI)
    db = client[DB_NAME]

    # Pre-cargar cachés en memoria para enriquecimiento ultrarrápido (< 1 ms)
    print("🧠 Cargando metadatos en caché de memoria para Stream Enrichment...", flush=True)
    usuarios_cache = {str(u["_id"]): u for u in db.usuarios.find({}, {"nombre": 1, "pais": 1, "segmento": 1, "codigo_usuario": 1})}
    peliculas_cache = {str(p["_id"]): p for p in db.peliculas.find({}, {"titulo": 1, "genero": 1, "duracion_minutos": 1, "anio": 1})}
    canales_cache = {str(c["_id"]): c for c in db.canales_tv.find({}, {"nombre": 1, "categoria": 1, "pais": 1})}
    print(f"✅ Caché lista: {len(usuarios_cache)} usuarios | {len(peliculas_cache)} películas | {len(canales_cache)} canales IPTV.\n", flush=True)

    # Contadores de métricas en tiempo real
    metricas = {
        "eventos_totales": 0,
        "plays": 0,
        "heartbeats": 0,
        "bufferings": 0,
        "completados": 0,
        "abandonos": 0,
        "latencias_acumuladas": 0,
        "inicio_tiempo": time.time()
    }

    # Pipeline de Change Stream: Escuchar únicamente inserciones en tiempo real
    pipeline_filtro = [
        {"$match": {"operationType": "insert"}}
    ]

    print("🟢 [LISTO] Escuchando flujo de eventos en tiempo real (Presiona Ctrl+C para detener)...", flush=True)
    print("-" * 78, flush=True)

    try:
        with db.eventos_reproduccion.watch(pipeline_filtro, full_document="updateLookup") as stream:
            for cambio in stream:
                evento = cambio["fullDocument"]
                metricas["eventos_totales"] += 1
                
                # 1. Extracción de atributos clave
                tipo_evento = evento.get("tipo_evento", "EVENT")
                usr_id_str = str(evento.get("usuario_id", ""))
                cnt_id_str = str(evento.get("contenido_id", ""))
                tipo_cont = evento.get("tipo_contenido", "Pelicula")
                dispositivo = evento.get("dispositivo", "Desconocido")
                bitrate = evento.get("bitrate_kbps", 0)
                latencia = evento.get("latencia_ms", 0)
                buffering_count = evento.get("eventos_buffering", 0)
                watch_sec = evento.get("segundo_reproduccion", 0)

                metricas["latencias_acumuladas"] += latencia

                # 2. Enriquecimiento al vuelo (Stream Enrichment con Caché en Memoria)
                info_usr = usuarios_cache.get(usr_id_str, {"nombre": "Usuario Anónimo", "pais": "Global", "segmento": "Regular"})
                
                if tipo_cont == "Canal_TV":
                    info_cnt = canales_cache.get(cnt_id_str, {"nombre": "Canal IPTV", "categoria": "En Vivo"})
                    titulo_cont = info_cnt.get("nombre", "Canal IPTV")
                    genero_cont = info_cnt.get("categoria", "En Vivo")
                    anio_cont = None
                else:
                    info_cnt = peliculas_cache.get(cnt_id_str, {"titulo": "Película VOD", "genero": "General", "duracion_minutos": 100})
                    titulo_cont = info_cnt.get("titulo", "Película VOD")
                    genero_cont = info_cnt.get("genero", "General")
                    anio_cont = info_cnt.get("anio", 2026)

                # 3. Clasificación de eventos
                tag_color = "🔵 [INFO]"
                if tipo_evento in ["PLAY", "STREAM_START"]:
                    metricas["plays"] += 1
                    tag_color = "🟢 [PLAY]"
                elif "HEARTBEAT" in tipo_evento:
                    metricas["heartbeats"] += 1
                    tag_color = "🟣 [HEARTBEAT]"
                elif tipo_evento in ["COMPLETE", "FINISH"]:
                    metricas["completados"] += 1
                    tag_color = "🎉 [COMPLETE]"
                elif tipo_evento in ["ABANDON", "DROP_OFF"]:
                    metricas["abandonos"] += 1
                    tag_color = "🟠 [ABANDONO]"

                # 4. Detección proactiva de Anomalías de QoS (Quality of Service Alert)
                alerta_qos = ""
                if buffering_count > 0 or latencia > 120:
                    metricas["bufferings"] += 1
                    alerta_qos = " ⚠️ [ALERTA QoS: Buffering/Latencia Alta]"

                # 5. Volcado reactivo a la capa OLAP (Sink Analítico)
                now_dt = datetime.now()
                doc_analitico = {
                    "codigo_evento": evento.get("codigo_evento", f"STR-CDC-{metricas['eventos_totales']}"),
                    "fecha_completa": now_dt,
                    "anio": now_dt.year,
                    "mes": now_dt.month,
                    "dia": now_dt.day,
                    "hora": now_dt.hour,
                    "dia_semana": now_dt.weekday() + 1,
                    "usuario": {
                        "id": evento.get("usuario_id"),
                        "nombre": info_usr.get("nombre"),
                        "pais": info_usr.get("pais"),
                        "segmento": info_usr.get("segmento"),
                        "plan": "Premium 4K"
                    },
                    "contenido": {
                        "id": evento.get("contenido_id"),
                        "tipo": tipo_cont,
                        "titulo": titulo_cont,
                        "genero_o_categoria": genero_cont,
                        "anio": anio_cont
                    },
                    "dispositivo": {
                        "tipo": dispositivo,
                        "sistema_operativo": evento.get("sistema_operativo", "OS"),
                        "resolucion": evento.get("resolucion", "1080p")
                    },
                    "duracion_vista_segundos": watch_sec,
                    "duracion_vista_minutos": round(watch_sec / 60, 1),
                    "porcentaje_visto": round(min(100.0, (watch_sec / (info_cnt.get("duracion_minutos", 100) * 60)) * 100), 1),
                    "completado": tipo_evento == "COMPLETE",
                    "abandono": tipo_evento == "ABANDON",
                    "tipo_evento": tipo_evento,
                    "bitrate_kbps": bitrate,
                    "latencia_ms": latencia,
                    "eventos_buffering": buffering_count,
                    "procesado_por": "Spark-Streaming-CDC-Engine"
                }
                db.reproducciones_analiticas.insert_one(doc_analitico)

                # 6. Salida en consola en tiempo real
                t_str = now_dt.strftime("%H:%M:%S.%f")[:-3]
                lat_prom = metricas["latencias_acumuladas"] / metricas["eventos_totales"]
                
                print(f"[{t_str}] {tag_color} #{metricas['eventos_totales']:04d} | "
                      f"{info_usr.get('nombre')[:14]:<14} ({info_usr.get('pais')[:3]}) ➔ "
                      f"'{titulo_cont[:18]}' | {dispositivo[:12]} | "
                      f"{bitrate} kbps | {latencia}ms{alerta_qos}", flush=True)

                # Cada 15 eventos mostrar tarjeta de KPIs en vivo
                if metricas["eventos_totales"] % 15 == 0:
                    tiempo_transcurrido = max(1.0, time.time() - metricas["inicio_tiempo"])
                    tps = metricas["eventos_totales"] / tiempo_transcurrido
                    print("-" * 78, flush=True)
                    print(f"📊 [KPIs EN VIVO] Throughput: {tps:.2f} ev/s | Latencia Media: {lat_prom:.1f}ms | "
                          f"Plays: {metricas['plays']} | Bufferings: {metricas['bufferings']} | "
                          f"Completados: {metricas['completados']}", flush=True)
                    print("-" * 78, flush=True)

    except KeyboardInterrupt:
        print("\n" + "=" * 78, flush=True)
        print("🛑 CONSUMIDOR DE STREAMING DETENIDO POR EL USUARIO", flush=True)
        tiempo_total = max(1.0, time.time() - metricas["inicio_tiempo"])
        print(f"⏱️  Tiempo activo: {tiempo_total:.1f} segundos", flush=True)
        print(f"📦 Total eventos procesados y enriquecidos: {metricas['eventos_totales']}", flush=True)
        print(f"⚡ Throughput promedio: {metricas['eventos_totales'] / tiempo_total:.2f} eventos/segundo", flush=True)
        print("=" * 78, flush=True)
    finally:
        client.close()

if __name__ == "__main__":
    iniciar_consumidor_streaming()
