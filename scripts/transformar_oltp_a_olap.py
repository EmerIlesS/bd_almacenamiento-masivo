import os
import sys
import time
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

def transformar_oltp_a_olap_hyperflix():
    print("=" * 70)
    print("🔄 HYPERFLIX: PIPELINE ETL / TRANSFORMACIÓN OLTP A OLAP")
    print("⭐ Modelo Dimensional: Estrella (FACT_REPRODUCCIONES + Dimensiones)")
    print("=" * 70 + "\n")
    
    client = pymongo.MongoClient(MONGO_URI)
    db = client[DB_NAME]
    inicio = time.time()
    
    total_eventos_oltp = db.eventos_reproduccion.count_documents({})
    print(f"📊 Registros OLTP detectados en 'eventos_reproduccion': {total_eventos_oltp}")
    
    if total_eventos_oltp == 0:
        print("⚠️ No hay eventos en 'eventos_reproduccion'. Ejecuta primero 'scripts/generar_datos_masivos.py'.")
        client.close()
        return

    # Pipeline de Agregación para desnormalizar y estructurar el Modelo en Estrella
    pipeline = [
        # 1. Unir con Dimensión Usuario (LEFT JOIN)
        {
            "$lookup": {
                "from": "usuarios",
                "localField": "usuario_id",
                "foreignField": "_id",
                "as": "info_usuario"
            }
        },
        {"$unwind": {"path": "$info_usuario", "preserveNullAndEmptyArrays": True}},
        
        # 2. Unir con Dimensión Suscripción para obtener plan activo
        {
            "$lookup": {
                "from": "suscripciones",
                "localField": "usuario_id",
                "foreignField": "usuario_id",
                "as": "info_suscripcion"
            }
        },
        
        # 3. Unir con Dimensión Películas
        {
            "$lookup": {
                "from": "peliculas",
                "localField": "contenido_id",
                "foreignField": "_id",
                "as": "info_pelicula"
            }
        },
        
        # 4. Unir con Dimensión Canales IPTV
        {
            "$lookup": {
                "from": "canales_tv",
                "localField": "contenido_id",
                "foreignField": "_id",
                "as": "info_canal"
            }
        },
        
        # 5. Proyectar y calcular métricas del Modelo en Estrella (FACT_REPRODUCCIONES)
        {
            "$project": {
                "_id": 0,
                "codigo_evento": "$codigo_evento",
                
                # Dimensión Temporal (DIM_FECHA)
                "fecha_completa": "$timestamp",
                "anio": {"$year": "$timestamp"},
                "mes": {"$month": "$timestamp"},
                "dia": {"$dayOfMonth": "$timestamp"},
                "hora": {"$hour": "$timestamp"},
                "dia_semana": {"$dayOfWeek": "$timestamp"},
                
                # Dimensión Usuario (DIM_USUARIO)
                "usuario": {
                    "id": "$info_usuario._id",
                    "codigo": "$info_usuario.codigo_usuario",
                    "nombre": "$info_usuario.nombre",
                    "pais": {"$ifNull": ["$info_usuario.pais", "Desconocido"]},
                    "ciudad": {"$ifNull": ["$info_usuario.ciudad", "Desconocido"]},
                    "segmento": {"$ifNull": ["$info_usuario.segmento", "Regular"]},
                    "plan": {
                        "$ifNull": [
                            {"$arrayElemAt": ["$info_suscripcion.plan", 0]},
                            "Básico"
                        ]
                    }
                },
                
                # Dimensión Contenido / Película (DIM_PELICULA / CONTENIDO)
                "contenido": {
                    "id": "$contenido_id",
                    "tipo": "$tipo_contenido",
                    "titulo": {
                        "$ifNull": [
                            {"$arrayElemAt": ["$info_pelicula.titulo", 0]},
                            {"$arrayElemAt": ["$info_canal.nombre", 0]}
                        ]
                    },
                    "genero_o_categoria": {
                        "$ifNull": [
                            {"$arrayElemAt": ["$info_pelicula.genero", 0]},
                            {"$arrayElemAt": ["$info_canal.categoria", 0]}
                        ]
                    },
                    "anio": {"$arrayElemAt": ["$info_pelicula.anio", 0]},
                    "duracion_total_seg": "$duracion_total_contenido_segundos"
                },
                
                # Dimensión Dispositivo (DIM_DISPOSITIVO)
                "dispositivo": {
                    "tipo": "$dispositivo",
                    "sistema_operativo": "$sistema_operativo",
                    "resolucion": "$resolucion"
                },
                
                # Hechos y Métricas Cuantitativas (FACT METRICS)
                "duracion_vista_segundos": "$duracion_sesion_segundos",
                "duracion_vista_minutos": {
                    "$round": [{"$divide": ["$duracion_sesion_segundos", 60]}, 1]
                },
                "porcentaje_visto": {
                    "$round": [
                        {
                            "$multiply": [
                                {
                                    "$divide": [
                                        "$duracion_sesion_segundos",
                                        {
                                            "$cond": [
                                                {"$gt": ["$duracion_total_contenido_segundos", 0]},
                                                "$duracion_total_contenido_segundos",
                                                1
                                            ]
                                        }
                                    ]
                                },
                                100
                            ]
                        },
                        1
                    ]
                },
                "completado": "$completado",
                "abandono": "$abandono",
                "tipo_evento": "$tipo_evento",
                
                # Métricas de Calidad de Servicio (QoS) y Red
                "bitrate_kbps": "$bitrate_kbps",
                "latencia_ms": "$latencia_ms",
                "eventos_buffering": "$eventos_buffering"
            }
        },
        
        # 6. Almacenar directamente en la colección analítica OLAP
        {
            "$merge": {
                "into": "reproducciones_analiticas",
                "whenNotMatched": "insert",
                "whenMatched": "replace"
            }
        }
    ]
    
    print("⚙️ Ejecutando Aggregation Pipeline (unión dimensional y cómputo de hechos)...")
    db.eventos_reproduccion.aggregate(pipeline, allowDiskUse=True)
    
    tiempo_total = time.time() - inicio
    total_olap = db.reproducciones_analiticas.count_documents({})
    
    print("\n" + "=" * 70)
    print("🎉 ¡TRANSFORMACIÓN OLTP A OLAP FINALIZADA EXITOSAMENTE!")
    print(f"⏱️  Tiempo de procesamiento: {tiempo_total:.2f} segundos")
    print(f"📊 Registros analíticos generados en 'reproducciones_analiticas': {total_olap}")
    print("=" * 70)
    
    client.close()

if __name__ == "__main__":
    transformar_oltp_a_olap_hyperflix()