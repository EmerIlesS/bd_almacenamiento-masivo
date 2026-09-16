import os
import sys
import random
import time
from datetime import datetime, timedelta
import pymongo
from dotenv import load_dotenv
from faker import Faker

# Configurar salida UTF-8 para consola Windows
if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8')
if hasattr(sys.stderr, 'reconfigure'):
    sys.stderr.reconfigure(encoding='utf-8')

load_dotenv()
MONGO_URI = os.getenv("MONGO_URI")
DB_NAME = os.getenv("MONGO_DB_NAME", "hyperflix_db")

fake = Faker(['es_CO', 'es_MX', 'es_ES'])

# Configuración de volúmenes de generación para simulación de laboratorio
NUM_USUARIOS = 1500
NUM_PELICULAS_NUEVAS = 60
NUM_CANALES_NUEVOS = 15
NUM_EVENTOS_STREAMING = 20000
BATCH_SIZE = 1000

GENEROS = [
    "Ciencia Ficción", "Acción", "Drama", "Comedia", "Terror",
    "Suspenso", "Animación", "Documental", "Aventura", "Crimen"
]

DIRECTORES = [
    "Christopher Nolan", "Denis Villeneuve", "Quentin Tarantino", "Guillermo del Toro",
    "Martin Scorsese", "Steven Spielberg", "Greta Gerwig", "Bong Joon-ho",
    "Alfonso Cuarón", "David Fincher", "Ridley Scott", "James Cameron"
]

DISPOSITIVOS = [
    {"tipo": "Smart TV Samsung", "so": "Tizen", "res": "3840x2160 (4K)", "bitrate_base": 18000, "lat_base": 30},
    {"tipo": "Smart TV LG", "so": "webOS", "res": "3840x2160 (4K)", "bitrate_base": 17000, "lat_base": 32},
    {"tipo": "Móvil Android", "so": "Android 14", "res": "1920x1080 (1080p)", "bitrate_base": 5500, "lat_base": 85},
    {"tipo": "iPhone", "so": "iOS 17", "res": "1920x1080 (1080p)", "bitrate_base": 6500, "lat_base": 70},
    {"tipo": "PC Web Chrome", "so": "Windows 11", "res": "1920x1080 (1080p)", "bitrate_base": 9000, "lat_base": 45},
    {"tipo": "PC Web Safari", "so": "macOS Sonoma", "res": "2560x1440 (2K)", "bitrate_base": 11000, "lat_base": 40},
    {"tipo": "Tablet iPad", "so": "iPadOS 17", "res": "2048x1536 (2K)", "bitrate_base": 8000, "lat_base": 60}
]

PAISES = ["Colombia", "México", "Argentina", "España", "Chile", "Perú"]
CIUDADES_POR_PAIS = {
    "Colombia": ["Bogotá", "Medellín", "Cali", "Barranquilla", "Mocoa", "Pasto"],
    "México": ["Ciudad de México", "Guadalajara", "Monterrey", "Puebla"],
    "Argentina": ["Buenos Aires", "Córdoba", "Rosario", "Mendoza"],
    "España": ["Madrid", "Barcelona", "Valencia", "Sevilla"],
    "Chile": ["Santiago", "Valparaíso", "Concepción"],
    "Perú": ["Lima", "Arequipa", "Cusco"]
}

PLANES = [
    {"nombre": "Premium 4K", "precio": 45000, "peso": 0.45},
    {"nombre": "Estándar HD", "precio": 32000, "peso": 0.35},
    {"nombre": "Básico", "precio": 19000, "peso": 0.20}
]

CANALES_IPTV_TEMAS = [
    ("Euronews Español", "Noticias", "Europa", "Español", "https://euronews-esp.akamaized.net/hls/live/2015530/euronews_es/index.m3u8"),
    ("Telemedellín En Vivo", "General / Regional", "Colombia", "Español", "https://stream.telemedellin.tv/live/telemedellin.m3u8"),
    ("Canal Institucional", "Educación / Cultura", "Colombia", "Español", "https://streaming.rtvc.gov.co/institucional_hls/live.m3u8"),
    ("Bloomberg Quicktake", "Economía / Finanzas", "Estados Unidos", "Inglés", "https://bloomberg.com/media-manifest/quicktake.m3u8"),
    ("Al Jazeera English", "Noticias Globales", "Qatar", "Inglés", "https://live-hls-web-aje.getaj.net/AJE/01.m3u8"),
    ("Canal Trece Colombia", "Música / Jóvenes", "Colombia", "Español", "https://streaming.canaltrece.com.co/live/trece.m3u8"),
    ("RT Documentales", "Documentales", "Internacional", "Español", "https://rt-esp.rttv.com/live/rtesp/playlist.m3u8"),
    ("Señal Colombia Deportes", "Deportes", "Colombia", "Español", "https://streaming.rtvc.gov.co/senalcolombia_hls/live.m3u8")
]

def conectar_bd():
    client = pymongo.MongoClient(MONGO_URI)
    return client[DB_NAME]

def insertar_en_lotes(coleccion, datos, nombre_entidad):
    """Inserta datos en bloques ordenados o desordenados para optimizar I/O"""
    total = len(datos)
    print(f"📦 Insertando {total} {nombre_entidad} en lotes de {BATCH_SIZE}...")
    
    for i in range(0, total, BATCH_SIZE):
        lote = datos[i:i + BATCH_SIZE]
        try:
            coleccion.insert_many(lote, ordered=False)
            progreso = min(i + BATCH_SIZE, total)
            print(f"  ➡️ Progreso: {progreso}/{total} ({(progreso/total)*100:.1f}%)")
        except pymongo.errors.BulkWriteError as e:
            insertados = e.details.get('nInserted', 0)
            progreso = min(i + BATCH_SIZE, total)
            print(f"  ⚠️ Progreso: {progreso}/{total} (Insertados {insertados})")
    
    print(f"  ✅ Proceso de {nombre_entidad} finalizado.\n")

def generar_datos_hyperflix():
    print("=" * 70)
    print("🚀 GENERADOR MASIVO DE DATOS: PLATAFORMA HYPERFLIX")
    print(f"🎯 Metas: {NUM_USUARIOS} usuarios, {NUM_PELICULAS_NUEVAS} películas, {NUM_EVENTOS_STREAMING} eventos de streaming")
    print("=" * 70 + "\n")
    
    db = conectar_bd()
    inicio_total = time.time()
    
    # 1. GENERAR PELÍCULAS ADICIONALES
    print("🎬 1/5. Generando catálogo ampliado de películas...")
    peliculas = []
    adjetivos = ["Oscuro", "Eterno", "Infinito", "Silencioso", "Cibernético", "Oculto", "Perdido", "Último", "Veloz", "Secreto"]
    sustantivos = ["Horizonte", "Destino", "Imperio", "Laberinto", "Protocolo", "Eclipse", "Viajero", "Espacio", "Reflejo", "Enigma"]
    
    for i in range(1, NUM_PELICULAS_NUEVAS + 1):
        titulo = f"{random.choice(adjetivos)} {random.choice(sustantivos)}: Parte {random.randint(1, 5)}" if random.random() > 0.6 else f"El {random.choice(sustantivos)} {random.choice(adjetivos)}"
        duracion = random.randint(80, 180) # minutos
        genero = random.choice(GENEROS)
        
        peliculas.append({
            "codigo_pelicula": f"MOV-{100 + i}",
            "titulo": titulo,
            "genero": genero,
            "duracion_minutos": duracion,
            "anio": random.randint(2000, 2026),
            "clasificacion": random.choice(["G", "PG", "PG-13", "R"]),
            "director": random.choice(DIRECTORES),
            "rating_promedio": round(random.uniform(5.8, 9.4), 1),
            "resolucion_maxima": random.choice(["4K", "1080p", "1080p", "720p"]),
            "url_stream": f"https://storage.hyperflix.io/vod/mov_{100+i}/master.m3u8",
            "url_portada": f"https://images.unsplash.com/photo-{1500000000000 + i}?w=400",
            "fecha_creacion": fake.date_time_between(start_date='-2y', end_date='now')
        })
    insertar_en_lotes(db.peliculas, peliculas, "películas")
    
    # 2. GENERAR CANALES IPTV ADICIONALES
    print("📺 2/5. Generando canales IPTV públicos...")
    canales = []
    for i, c in enumerate(CANALES_IPTV_TEMAS):
        nombre, cat, pais, idioma, url = c
        canales.append({
            "codigo_canal": f"IPTV-{10 + i}",
            "nombre": nombre,
            "categoria": cat,
            "pais": pais,
            "idioma": idioma,
            "url_stream": url,
            "stream_hls_directo": url,
            "logo_url": f"https://images.hyperflix.io/logos/iptv_{10+i}.png",
            "activo": True,
            "tipo": "IPTV Público Libre"
        })
    insertar_en_lotes(db.canales_tv, canales, "canales IPTV")
    
    # 3. GENERAR USUARIOS, SUSCRIPCIONES Y PAGOS
    print("👥 3/5. Generando usuarios, suscripciones y transacciones de pago...")
    usuarios = []
    segmentos = ["Cinéfilo 4K", "Móvil Frecuente", "Casual", "Fin de Semana", "Familiar", "TV Noticias"]
    
    for i in range(1, NUM_USUARIOS + 1):
        pais = random.choice(PAISES)
        ciudad = random.choice(CIUDADES_POR_PAIS[pais])
        fecha_reg = fake.date_time_between(start_date='-18m', end_date='now')
        disp = random.choice(DISPOSITIVOS)["tipo"]
        
        usuarios.append({
            "codigo_usuario": f"USR-{10000 + i}",
            "nombre": fake.name(),
            "correo": f"{fake.user_name()}{random.randint(10, 9999)}@{fake.free_email_domain()}",
            "contraseña_hash": fake.sha256(),
            "pais": pais,
            "ciudad": ciudad,
            "segmento": random.choice(segmentos),
            "dispositivo_frecuente": disp,
            "fecha_registro": fecha_reg,
            "activo": random.choice([True, True, True, True, False])
        })
    insertar_en_lotes(db.usuarios, usuarios, "usuarios")
    
    # Recuperar IDs generados de usuarios
    usuarios_db = list(db.usuarios.find({}, {"_id": 1, "fecha_registro": 1, "pais": 1}))
    
    # Generar suscripciones y pagos para cada usuario
    suscripciones = []
    pagos = []
    metodos_pago = ["Tarjeta de Crédito", "PSE", "Nequi", "DaviPlata", "PayPal", "Efectivo"]
    
    for u in usuarios_db:
        # Ponderación de planes
        r_plan = random.random()
        if r_plan < 0.45:
            plan_obj = PLANES[0] # Premium
        elif r_plan < 0.80:
            plan_obj = PLANES[1] # Estándar
        else:
            plan_obj = PLANES[2] # Básico
            
        estado_sub = random.choice(["Activa", "Activa", "Activa", "Vencida", "Cancelada"])
        fecha_sub = u["fecha_registro"] + timedelta(minutes=random.randint(5, 60))
        
        suscripciones.append({
            "usuario_id": u["_id"],
            "plan": plan_obj["nombre"],
            "estado": estado_sub,
            "precio_mensual": plan_obj["precio"],
            "moneda": "COP",
            "fecha_inicio": fecha_sub,
            "fecha_fin": fecha_sub + timedelta(days=365),
            "renovacion_automatica": estado_sub == "Activa"
        })
        
        # Generar entre 1 y 4 pagos históricos por usuario
        num_pagos = random.randint(1, 4)
        for p_idx in range(num_pagos):
            fecha_pago = fecha_sub + timedelta(days=30 * p_idx)
            if fecha_pago > datetime.now():
                break
            pagos.append({
                "usuario_id": u["_id"],
                "monto": plan_obj["precio"],
                "metodo": random.choice(metodos_pago),
                "fecha": fecha_pago,
                "estado": random.choice(["Completado", "Completado", "Completado", "Rechazado"])
            })
            
    insertar_en_lotes(db.suscripciones, suscripciones, "suscripciones")
    insertar_en_lotes(db.pagos, pagos, "pagos")
    
    # 4. GENERAR EVENTOS MASIVOS DE STREAMING Y TELEMETRÍA (INGESTA KAFKA / REPRODUCCIONES)
    print("📡 4/5. Generando eventos masivos de telemetría y streaming...")
    peliculas_db = list(db.peliculas.find({}, {"_id": 1, "titulo": 1, "duracion_minutos": 1, "genero": 1}))
    canales_db = list(db.canales_tv.find({}, {"_id": 1, "nombre": 1, "categoria": 1}))
    
    eventos_streaming = []
    
    # Simulamos sesiones de streaming realistas
    for i in range(1, NUM_EVENTOS_STREAMING + 1):
        u = random.choice(usuarios_db)
        disp = random.choice(DISPOSITIVOS)
        
        # 85% películas VOD, 15% canales de TV en vivo
        es_pelicula = random.random() < 0.85
        
        if es_pelicula:
            item = random.choice(peliculas_db)
            duracion_total_seg = item["duracion_minutos"] * 60
            tipo_contenido = "Pelicula"
            contenido_id = item["_id"]
        else:
            item = random.choice(canales_db)
            duracion_total_seg = random.randint(300, 7200) # tiempo en canal en vivo
            tipo_contenido = "Canal_TV"
            contenido_id = item["_id"]
        
        # Tipo de evento y comportamiento de visualización
        # Comportamiento: Abandono temprano (20%), Vista parcial (40%), Vista completa (40%)
        comportamiento = random.random()
        if comportamiento < 0.20:
            # Abandono temprano (menos del 20% del contenido)
            duracion_vista = random.randint(30, int(duracion_total_seg * 0.20))
            tipo_evento = "ABANDON"
            completado = False
            abandono = True
        elif comportamiento < 0.60:
            # Vista parcial
            duracion_vista = random.randint(int(duracion_total_seg * 0.21), int(duracion_total_seg * 0.84))
            tipo_evento = random.choice(["PAUSE", "STOP", "SEEK"])
            completado = False
            abandono = False
        else:
            # Vista completa
            duracion_vista = int(duracion_total_seg * random.uniform(0.85, 1.0))
            tipo_evento = "COMPLETE"
            completado = True
            abandono = False
            
        timestamp_evento = fake.date_time_between(start_date='-6m', end_date='now')
        
        # Métricas de QoS (Quality of Service)
        latencia = int(disp["lat_base"] * random.uniform(0.8, 2.5))
        bitrate = int(disp["bitrate_base"] * random.uniform(0.7, 1.1))
        hubo_buffering = random.random() < 0.08 # 8% experimentó buffering
        veces_buffering = random.randint(1, 5) if hubo_buffering else 0
        
        eventos_streaming.append({
            "codigo_evento": f"STR-{100000 + i}",
            "usuario_id": u["_id"],
            "contenido_id": contenido_id,
            "tipo_contenido": tipo_contenido,
            "tipo_evento": tipo_evento,
            "dispositivo": disp["tipo"],
            "sistema_operativo": disp["so"],
            "resolucion": disp["res"],
            "timestamp": timestamp_evento,
            "segundo_reproduccion": duracion_vista,
            "duracion_sesion_segundos": duracion_vista,
            "duracion_total_contenido_segundos": duracion_total_seg,
            "completado": completado,
            "abandono": abandono,
            "bitrate_kbps": bitrate,
            "latencia_ms": latencia,
            "eventos_buffering": veces_buffering
        })
        
        if len(eventos_streaming) >= BATCH_SIZE:
            insertar_en_lotes(db.eventos_reproduccion, eventos_streaming, "eventos de streaming (lote)")
            eventos_streaming = []
            
    if len(eventos_streaming) > 0:
        insertar_en_lotes(db.eventos_reproduccion, eventos_streaming, "eventos de streaming (final)")
        
    # 5. RESUMEN FINAL
    tiempo_total = time.time() - inicio_total
    print("=" * 70)
    print("🎉 ¡GENERACIÓN MASIVA DE HYPERFLIX COMPLETADA EXITOSAMENTE!")
    print(f"⏱️  Tiempo total de ejecución: {tiempo_total:.2f} segundos")
    print(f"👤 Usuarios registrados: {db.usuarios.count_documents({})}")
    print(f"🎬 Películas en catálogo: {db.peliculas.count_documents({})}")
    print(f"📺 Canales IPTV activos: {db.canales_tv.count_documents({})}")
    print(f"💳 Suscripciones activas/totales: {db.suscripciones.count_documents({})}")
    print(f"💰 Transacciones de pago: {db.pagos.count_documents({})}")
    print(f"📡 Eventos de telemetría/streaming: {db.eventos_reproduccion.count_documents({})}")
    print("=" * 70)
    
    db.client.close()

if __name__ == "__main__":
    generar_datos_hyperflix()