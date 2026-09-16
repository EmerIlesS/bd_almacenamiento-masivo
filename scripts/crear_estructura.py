import os
import sys
from datetime import datetime, timedelta
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

def crear_estructura_hyperflix():
    print("=" * 70)
    print("🎬 HYPERFLIX: INICIALIZACIÓN DE ARQUITECTURA DE DATOS (FASE 1)")
    print("=" * 70)
    
    client = pymongo.MongoClient(MONGO_URI)
    db = client[DB_NAME]
    
    print(f"📦 Conectado a base de datos: '{DB_NAME}'\n")
    
    # 1. Limpiar colecciones previas para inicialización limpia
    colecciones_actuales = db.list_collection_names()
    colecciones_hyperflix = [
        "usuarios", "suscripciones", "peliculas", "canales_tv",
        "pagos", "eventos_reproduccion", "reproducciones_analiticas"
    ]
    
    for col in colecciones_hyperflix:
        if col in colecciones_actuales:
            db[col].drop()
            print(f"  🗑️ Colección anterior '{col}' reiniciada.")
    
    # 2. Crear colecciones OLTP (Transaccionales y de Ingesta)
    print("\n📦 Creando colecciones OLTP e Ingesta...")
    colecciones_oltp = [
        "usuarios", "suscripciones", "peliculas", "canales_tv",
        "pagos", "eventos_reproduccion"
    ]
    for col in colecciones_oltp:
        db.create_collection(col)
        print(f"  ✅ Colección OLTP '{col}' creada")
    
    # 3. Crear colección OLAP (Analítica / Data Warehouse)
    print("\n📊 Creando colección OLAP (Modelo Estrella - FACT_REPRODUCCIONES)...")
    db.create_collection("reproducciones_analiticas")
    print("  ✅ Colección OLAP 'reproducciones_analiticas' creada")
    
    # 4. Crear Índices de Optimización
    print("\n🔍 Creando índices de optimización para OLTP y Streaming...")
    # Usuarios
    db.usuarios.create_index([("correo", 1)], unique=True, name="idx_usuario_correo")
    db.usuarios.create_index([("codigo_usuario", 1)], unique=True, name="idx_codigo_usuario")
    db.usuarios.create_index([("pais", 1), ("segmento", 1)], name="idx_pais_segmento")
    
    # Suscripciones
    db.suscripciones.create_index([("usuario_id", 1)], name="idx_sub_usuario")
    db.suscripciones.create_index([("estado", 1), ("plan", 1)], name="idx_sub_estado_plan")
    
    # Películas
    db.peliculas.create_index([("codigo_pelicula", 1)], unique=True, name="idx_codigo_pelicula")
    db.peliculas.create_index([("genero", 1), ("anio", -1)], name="idx_pelicula_genero_anio")
    
    # Canales IPTV
    db.canales_tv.create_index([("codigo_canal", 1)], unique=True, name="idx_codigo_canal")
    db.canales_tv.create_index([("categoria", 1), ("pais", 1)], name="idx_canal_categoria_pais")
    
    # Pagos
    db.pagos.create_index([("usuario_id", 1), ("fecha", -1)], name="idx_pago_usuario_fecha")
    
    # Eventos de Reproducción (Streaming Ingestion)
    db.eventos_reproduccion.create_index([("usuario_id", 1), ("timestamp", -1)], name="idx_eventos_usr_time")
    db.eventos_reproduccion.create_index([("contenido_id", 1), ("tipo_evento", 1)], name="idx_eventos_content_type")
    db.eventos_reproduccion.create_index([("timestamp", -1)], name="idx_eventos_timestamp")
    print("  ✅ Índices OLTP creados exitosamente.")
    
    # 5. Insertar Datos Semilla (Seed Data)
    print("\n🌱 Insertando datos semilla demostrativos...")
    
    # 5.1 Catálogo de Películas Semilla
    peliculas_semilla = [
        {
            "codigo_pelicula": "MOV-001",
            "titulo": "Inception",
            "genero": "Ciencia Ficción",
            "duracion_minutos": 148,
            "anio": 2010,
            "clasificacion": "PG-13",
            "director": "Christopher Nolan",
            "rating_promedio": 8.8,
            "resolucion_maxima": "4K",
            "url_stream": "https://storage.hyperflix.io/vod/inception/master.m3u8",
            "url_portada": "https://images.unsplash.com/photo-1536440136628-849c177e76a1?w=400",
            "fecha_creacion": datetime.now()
        },
        {
            "codigo_pelicula": "MOV-002",
            "titulo": "Interstellar",
            "genero": "Ciencia Ficción",
            "duracion_minutos": 169,
            "anio": 2014,
            "clasificacion": "PG-13",
            "director": "Christopher Nolan",
            "rating_promedio": 8.7,
            "resolucion_maxima": "4K",
            "url_stream": "https://storage.hyperflix.io/vod/interstellar/master.m3u8",
            "url_portada": "https://images.unsplash.com/photo-1451187580459-43490279c0fa?w=400",
            "fecha_creacion": datetime.now()
        },
        {
            "codigo_pelicula": "MOV-003",
            "titulo": "The Dark Knight",
            "genero": "Acción",
            "duracion_minutos": 152,
            "anio": 2008,
            "clasificacion": "PG-13",
            "director": "Christopher Nolan",
            "rating_promedio": 9.0,
            "resolucion_maxima": "4K",
            "url_stream": "https://storage.hyperflix.io/vod/dark_knight/master.m3u8",
            "url_portada": "https://images.unsplash.com/photo-1509198397868-475647b2a1e5?w=400",
            "fecha_creacion": datetime.now()
        },
        {
            "codigo_pelicula": "MOV-004",
            "titulo": "Pulp Fiction",
            "genero": "Crimen",
            "duracion_minutos": 154,
            "anio": 1994,
            "clasificacion": "R",
            "director": "Quentin Tarantino",
            "rating_promedio": 8.9,
            "resolucion_maxima": "1080p",
            "url_stream": "https://storage.hyperflix.io/vod/pulp_fiction/master.m3u8",
            "url_portada": "https://images.unsplash.com/photo-1594909122845-11baa439b7bf?w=400",
            "fecha_creacion": datetime.now()
        },
        {
            "codigo_pelicula": "MOV-005",
            "titulo": "Parasite",
            "genero": "Drama",
            "duracion_minutos": 132,
            "anio": 2019,
            "clasificacion": "R",
            "director": "Bong Joon-ho",
            "rating_promedio": 8.5,
            "resolucion_maxima": "4K",
            "url_stream": "https://storage.hyperflix.io/vod/parasite/master.m3u8",
            "url_portada": "https://images.unsplash.com/photo-1489599849927-2ee91cede3ba?w=400",
            "fecha_creacion": datetime.now()
        },
        {
            "codigo_pelicula": "MOV-006",
            "titulo": "Cyberpunk 2099: Rebirth",
            "genero": "Animación",
            "duracion_minutos": 115,
            "anio": 2024,
            "clasificacion": "PG-13",
            "director": "Elena Rostova",
            "rating_promedio": 8.3,
            "resolucion_maxima": "4K",
            "url_stream": "https://storage.hyperflix.io/vod/cyberpunk2099/master.m3u8",
            "url_portada": "https://images.unsplash.com/photo-1578632767115-351597cf2477?w=400",
            "fecha_creacion": datetime.now()
        }
    ]
    res_pelis = db.peliculas.insert_many(peliculas_semilla)
    print(f"  🎬 {len(res_pelis.inserted_ids)} películas insertadas.")
    
    # 5.2 Catálogo de Canales IPTV Públicos y Libres
    canales_semilla = [
        {
            "codigo_canal": "IPTV-001",
            "nombre": "France 24 Español",
            "categoria": "Noticias",
            "pais": "Francia / Internacional",
            "idioma": "Español",
            "url_stream": "https://raw.githubusercontent.com/iptv-org/iptv/master/streams/fr_f24es.m3u",
            "stream_hls_directo": "https://f24hls-i.akamaihd.net/hls/live/221193/F24_ES_LO_HLS/master_500.m3u8",
            "logo_url": "https://upload.wikimedia.org/wikipedia/commons/4/41/France24_2013.svg",
            "activo": True,
            "tipo": "IPTV Público Libre"
        },
        {
            "codigo_canal": "IPTV-002",
            "nombre": "DW Español (Deutsche Welle)",
            "categoria": "Noticias / Documentales",
            "pais": "Alemania",
            "idioma": "Español",
            "url_stream": "https://dwamdstream104.akamaized.net/hls/live/2015530/dwstream104/index.m3u8",
            "stream_hls_directo": "https://dwamdstream104.akamaized.net/hls/live/2015530/dwstream104/index.m3u8",
            "logo_url": "https://upload.wikimedia.org/wikipedia/commons/7/75/Deutsche_Welle_logo.svg",
            "activo": True,
            "tipo": "IPTV Público Libre"
        },
        {
            "codigo_canal": "IPTV-003",
            "nombre": "RTVE 24h España",
            "categoria": "Noticias",
            "pais": "España",
            "idioma": "Español",
            "url_stream": "https://ztnr.rtve.es/ztnr/1694255.m3u8",
            "stream_hls_directo": "https://ztnr.rtve.es/ztnr/1694255.m3u8",
            "logo_url": "https://upload.wikimedia.org/wikipedia/commons/e/e4/Canal_24_horas_2008.svg",
            "activo": True,
            "tipo": "IPTV Público Libre"
        },
        {
            "codigo_canal": "IPTV-004",
            "nombre": "NASA TV Public HD",
            "categoria": "Ciencia / Espacio",
            "pais": "Estados Unidos",
            "idioma": "Inglés",
            "url_stream": "https://ntv1.akamaized.net/hls/live/2014075/NASA-NTV1-HLS/master.m3u8",
            "stream_hls_directo": "https://ntv1.akamaized.net/hls/live/2014075/NASA-NTV1-HLS/master.m3u8",
            "logo_url": "https://upload.wikimedia.org/wikipedia/commons/e/e5/NASA_logo.svg",
            "activo": True,
            "tipo": "IPTV Público Libre"
        },
        {
            "codigo_canal": "IPTV-005",
            "nombre": "Red Bull TV",
            "categoria": "Deportes Extremos",
            "pais": "Austria / Global",
            "idioma": "Inglés / Español",
            "url_stream": "https://rbmn-live.akamaized.net/hls/live/590964/BoRB-AT/master.m3u8",
            "stream_hls_directo": "https://rbmn-live.akamaized.net/hls/live/590964/BoRB-AT/master.m3u8",
            "logo_url": "https://upload.wikimedia.org/wikipedia/commons/7/7b/Red_Bull_logo.svg",
            "activo": True,
            "tipo": "IPTV Público Libre"
        }
    ]
    res_canales = db.canales_tv.insert_many(canales_semilla)
    print(f"  📺 {len(res_canales.inserted_ids)} canales IPTV públicos insertados.")
    
    # 5.3 Usuarios Semilla
    usuarios_semilla = [
        {
            "codigo_usuario": "USR-00001",
            "nombre": "Carlos Mendoza",
            "correo": "carlos.mendoza@hyperflix.io",
            "contraseña_hash": "sha256$e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
            "pais": "Colombia",
            "ciudad": "Bogotá",
            "segmento": "Cinéfilo Premium",
            "dispositivo_frecuente": "Smart TV Samsung",
            "fecha_registro": datetime.now() - timedelta(days=120),
            "activo": True
        },
        {
            "codigo_usuario": "USR-00002",
            "nombre": "Valentina Gómez",
            "correo": "valen.gomez@hyperflix.io",
            "contraseña_hash": "sha256$ca978112ca1bbdcafac231b39a23dc4da7860814961409fa49053896c21e6be5",
            "pais": "México",
            "ciudad": "Ciudad de México",
            "segmento": "Móvil Frecuente",
            "dispositivo_frecuente": "Móvil Android",
            "fecha_registro": datetime.now() - timedelta(days=60),
            "activo": True
        },
        {
            "codigo_usuario": "USR-00003",
            "nombre": "Mateo Rossi",
            "correo": "mateo.rossi@hyperflix.io",
            "contraseña_hash": "sha256$5e884898da28047151d0e56f8dc6292773603d0d6aabbdd62a11ef721d1542d8",
            "pais": "Argentina",
            "ciudad": "Buenos Aires",
            "segmento": "Casual",
            "dispositivo_frecuente": "PC Web Chrome",
            "fecha_registro": datetime.now() - timedelta(days=15),
            "activo": True
        }
    ]
    res_usr = db.usuarios.insert_many(usuarios_semilla)
    print(f"  👤 {len(res_usr.inserted_ids)} usuarios semilla insertados.")
    
    # 5.4 Suscripciones y Pagos asociados a los usuarios semilla
    usr_ids = list(db.usuarios.find({}, {"_id": 1, "codigo_usuario": 1}))
    planes = ["Premium 4K", "Estándar HD", "Básico"]
    precios = [45000, 32000, 19000]
    
    suscripciones = []
    pagos = []
    
    for i, u in enumerate(usr_ids):
        plan_elegido = planes[i % len(planes)]
        monto = precios[i % len(precios)]
        fecha_ini = datetime.now() - timedelta(days=30 * (i + 1))
        
        sub = {
            "codigo_suscripcion": f"SUB-0000{i+1}",
            "usuario_id": u["_id"],
            "plan": plan_elegido,
            "estado": "Activa",
            "precio_mensual": monto,
            "moneda": "COP",
            "fecha_inicio": fecha_ini,
            "fecha_fin": fecha_ini + timedelta(days=365),
            "renovacion_automatica": True
        }
        res_s = db.suscripciones.insert_one(sub)
        
        pago = {
            "codigo_pago": f"PAY-2026-000{i+1}",
            "usuario_id": u["_id"],
            "suscripcion_id": res_s.inserted_id,
            "monto": monto,
            "metodo": "Tarjeta de Crédito" if i % 2 == 0 else "PSE",
            "fecha": fecha_ini,
            "estado": "Completado"
        }
        pagos.append(pago)
        
    db.pagos.insert_many(pagos)
    print(f"  💳 {len(pagos)} suscripciones y transacciones de pago generadas.")
    
    # 5.5 Eventos de Streaming semilla demostrativos
    pelicula_inception = db.peliculas.find_one({"codigo_pelicula": "MOV-001"})
    canal_dw = db.canales_tv.find_one({"codigo_canal": "IPTV-002"})
    
    eventos_demo = [
        {
            "codigo_evento": "EVT-0001",
            "usuario_id": usr_ids[0]["_id"],
            "contenido_id": pelicula_inception["_id"],
            "tipo_contenido": "Pelicula",
            "tipo_evento": "PLAY",
            "dispositivo": "Smart TV Samsung",
            "sistema_operativo": "Tizen",
            "timestamp": datetime.now() - timedelta(hours=3),
            "segundo_reproduccion": 0,
            "duracion_sesion_segundos": 0,
            "bitrate_kbps": 15000,
            "latencia_ms": 42,
            "resolucion": "3840x2160 (4K)"
        },
        {
            "codigo_evento": "EVT-0002",
            "usuario_id": usr_ids[0]["_id"],
            "contenido_id": pelicula_inception["_id"],
            "tipo_contenido": "Pelicula",
            "tipo_evento": "COMPLETE",
            "dispositivo": "Smart TV Samsung",
            "sistema_operativo": "Tizen",
            "timestamp": datetime.now() - timedelta(hours=1),
            "segundo_reproduccion": 8880,
            "duracion_sesion_segundos": 8880,
            "bitrate_kbps": 15000,
            "latencia_ms": 38,
            "resolucion": "3840x2160 (4K)"
        },
        {
            "codigo_evento": "EVT-0003",
            "usuario_id": usr_ids[1]["_id"],
            "contenido_id": canal_dw["_id"],
            "tipo_contenido": "Canal_TV",
            "tipo_evento": "PLAY",
            "dispositivo": "Móvil Xiaomi",
            "sistema_operativo": "Android 14",
            "timestamp": datetime.now() - timedelta(minutes=45),
            "segundo_reproduccion": 0,
            "duracion_sesion_segundos": 1200,
            "bitrate_kbps": 4500,
            "latencia_ms": 85,
            "resolucion": "1920x1080 (1080p)"
        }
    ]
    db.eventos_reproduccion.insert_many(eventos_demo)
    print(f"  📡 {len(eventos_demo)} eventos de telemetría semilla insertados.")
    
    print("\n" + "=" * 70)
    print("🎉 ¡ARQUITECTURA DE DATOS HYPERFLIX INICIALIZADA EXITOSAMENTE!")
    print("=" * 70)
    print(f"📊 Colecciones creadas en '{DB_NAME}':")
    for col in db.list_collection_names():
        total_docs = db[col].count_documents({})
        print(f"   • {col}: {total_docs} documentos")
    print("=" * 70)
    
    client.close()

if __name__ == "__main__":
    crear_estructura_hyperflix()