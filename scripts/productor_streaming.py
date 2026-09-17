import os
import sys
import time
import random
import argparse
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

DISPOSITIVOS = [
    {"tipo": "Smart TV Samsung", "so": "Tizen", "res": "3840x2160 (4K)", "bitrate": 18500, "lat": 28},
    {"tipo": "Smart TV LG", "so": "webOS", "res": "3840x2160 (4K)", "bitrate": 16500, "lat": 32},
    {"tipo": "Móvil Android", "so": "Android 14", "res": "1920x1080 (1080p)", "bitrate": 5500, "lat": 85},
    {"tipo": "iPhone 15 Pro", "so": "iOS 17", "res": "1920x1080 (1080p)", "bitrate": 6800, "lat": 65},
    {"tipo": "PC Web Chrome", "so": "Windows 11", "res": "1920x1080 (1080p)", "bitrate": 9500, "lat": 42},
    {"tipo": "iPad Pro", "so": "iPadOS 17", "res": "2048x1536 (2K)", "bitrate": 8200, "lat": 55}
]

TIPOS_EVENTOS = [
    ("PLAY", 0.35),
    ("HEARTBEAT_10S", 0.40),
    ("SEEK", 0.08),
    ("BUFFERING_ERROR", 0.07),
    ("COMPLETE", 0.05),
    ("ABANDON", 0.05)
]

def elegir_tipo_evento():
    r = random.random()
    acum = 0.0
    for tipo, peso in TIPOS_EVENTOS:
        acum += peso
        if r <= acum:
            return tipo
    return "HEARTBEAT_10S"

def iniciar_productor_streaming(cantidad=None, intervalo=0.8):
    print("=" * 78)
    print("🚀 HYPERFLIX: PRODUCTOR DE EVENT STREAMING EN TIEMPO REAL")
    print("🎯 Simulación Arquitectónica: Emisor de Telemetría de Clientes ➔ Broker Apache Kafka")
    print(f"📦 Destino: MongoDB Atlas ('{DB_NAME}.eventos_reproduccion')")
    if cantidad:
        print(f"⏱️ Modo: Lote de {cantidad} eventos | Intervalo: {intervalo} segundos")
    else:
        print(f"⏱️ Modo: Emisión Continua (Presiona Ctrl+C para detener) | Intervalo: {intervalo}s")
    print("=" * 78 + "\n")

    client = pymongo.MongoClient(MONGO_URI)
    db = client[DB_NAME]

    usuarios_db = list(db.usuarios.find({}, {"_id": 1, "nombre": 1}))
    peliculas_db = list(db.peliculas.find({}, {"_id": 1, "titulo": 1, "duracion_minutos": 1}))
    canales_db = list(db.canales_tv.find({}, {"_id": 1, "nombre": 1}))

    if not usuarios_db or not peliculas_db:
        print("❌ Error: No se encontraron usuarios o películas en la base de datos.")
        client.close()
        return

    print(f"👥 Pool de simulación: {len(usuarios_db)} usuarios | 🎬 {len(peliculas_db)} películas | 📺 {len(canales_db)} canales IPTV\n")
    print("🟢 [INICIANDO] Generando y enviando eventos de streaming a Atlas...")
    print("-" * 78)

    contador = 0
    try:
        while True:
            contador += 1
            u = random.choice(usuarios_db)
            disp = random.choice(DISPOSITIVOS)
            tipo_evento = elegir_tipo_evento()

            # 85% Película, 15% Canal IPTV
            es_pelicula = random.random() < 0.85
            if es_pelicula:
                item = random.choice(peliculas_db)
                tipo_cont = "Pelicula"
                cnt_id = item["_id"]
                titulo_log = item["titulo"]
                dur_total = item["duracion_minutos"] * 60
            else:
                item = random.choice(canales_db)
                tipo_cont = "Canal_TV"
                cnt_id = item["_id"]
                titulo_log = item["nombre"]
                dur_total = 3600

            # Métricas dinámicas según evento
            if tipo_evento == "PLAY":
                watch_sec = 0
                buffering_count = 0
                latencia = int(disp["lat"] * random.uniform(0.9, 1.2))
            elif tipo_evento == "BUFFERING_ERROR":
                watch_sec = random.randint(30, int(dur_total * 0.5))
                buffering_count = random.randint(1, 4)
                latencia = int(disp["lat"] * random.uniform(2.5, 4.5)) # Pico de latencia
            elif tipo_evento == "COMPLETE":
                watch_sec = int(dur_total * random.uniform(0.92, 1.0))
                buffering_count = 0
                latencia = int(disp["lat"] * random.uniform(0.8, 1.1))
            elif tipo_evento == "ABANDON":
                watch_sec = random.randint(20, int(dur_total * 0.18))
                buffering_count = 1 if random.random() < 0.5 else 0
                latencia = int(disp["lat"] * random.uniform(1.0, 1.8))
            else: # HEARTBEAT o SEEK
                watch_sec = random.randint(60, int(dur_total * 0.8))
                buffering_count = 0
                latencia = int(disp["lat"] * random.uniform(0.9, 1.3))

            bitrate_dinamico = int(disp["bitrate"] * random.uniform(0.85, 1.05))

            doc_evento = {
                "codigo_evento": f"STR-LIVE-{int(time.time()*1000)%1000000:06d}",
                "usuario_id": u["_id"],
                "contenido_id": cnt_id,
                "tipo_contenido": tipo_cont,
                "tipo_evento": tipo_evento,
                "dispositivo": disp["tipo"],
                "sistema_operativo": disp["so"],
                "resolucion": disp["res"],
                "timestamp": datetime.now(),
                "segundo_reproduccion": watch_sec,
                "duracion_sesion_segundos": watch_sec,
                "duracion_total_contenido_segundos": dur_total,
                "completado": tipo_evento == "COMPLETE",
                "abandono": tipo_evento == "ABANDON",
                "bitrate_kbps": bitrate_dinamico,
                "latencia_ms": latencia,
                "eventos_buffering": buffering_count
            }

            # Inserción en MongoDB Atlas (Desencadena instantáneamente el Change Stream en el Consumidor)
            db.eventos_reproduccion.insert_one(doc_evento)

            now_str = datetime.now().strftime("%H:%M:%S.%f")[:-3]
            print(f"[{now_str}] 📤 Emitido #{contador:04d} ➔ {tipo_evento:<16} | "
                  f"Usuario: {u['nombre'][:14]:<14} | '{titulo_log[:18]}' | "
                  f"{disp['tipo'][:12]} | {bitrate_dinamico}kbps | {latencia}ms")

            if cantidad and contador >= cantidad:
                break

            time.sleep(intervalo * random.uniform(0.7, 1.3))

    except KeyboardInterrupt:
        print("\n" + "=" * 78)
        print("🛑 PRODUCTOR DETENIDO POR EL USUARIO")
        print(f"📦 Total eventos enviados: {contador}")
        print("=" * 78)
    finally:
        client.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Productor de Streaming en Tiempo Real para HyperFlix")
    parser.add_argument("--count", type=int, default=None, help="Número de eventos a enviar (None para infinito)")
    parser.add_argument("--delay", type=float, default=0.8, help="Intervalo de tiempo entre eventos en segundos")
    args = parser.parse_args()

    iniciar_productor_streaming(cantidad=args.count, intervalo=args.delay)
