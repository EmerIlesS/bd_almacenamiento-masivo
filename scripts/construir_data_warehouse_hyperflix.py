"""
=============================================================================
FASE 3: DATA WAREHOUSE HYPERFLIX (MODELO EN ESTRELLA / STAR SCHEMA)
=============================================================================
Asignatura: Bases de Datos y Almacenamiento Masivo (Octavo Semestre)
Dominio del Proyecto: HYPERFLIX (Streaming Masivo de Video, VOD e IPTV)

Equivalencia del Modelo Dimensional respecto a la Guía del Docente:
  • Fuente:       ventas_analiticas     ➔ reproducciones_analiticas
  • dim_tiempo:   dim_tiempo            ➔ dim_tiempo (Fechas, Trimestre $ceil, Fin de Semana $cond, Horas Pico)
  • dim_cliente:  dim_cliente           ➔ dim_usuario (Usuarios, Países, Planes, Horas Vistas)
  • dim_producto: dim_producto          ➔ dim_contenido (Catálogo Películas / IPTV, Géneros, Audiencia)
  • dim_extra:    -                     ➔ dim_dispositivo (Smart TV, Móvil, PC, QoS por SO)
  • fact_ventas:  fact_ventas           ➔ fact_reproducciones (Claves foráneas + Métricas de Telemetría)

Operadores y Técnicas Evaluadas:
  • $group, $project, $merge
  • $ceil y $divide (Cálculo de trimestre)
  • $cond (Lógica condicional para fines de semana y horas pico)
  • $first (Captura de atributos estáticos únicos)
  • $sum, $avg, $round (Métricas cuantitativas y redondeo analítico)
  • Optimización Big Data: allowDiskUse=True
  • Auditoría final: count_documents({})
=============================================================================
"""

import os
import sys
import time
import pymongo
from dotenv import load_dotenv

# Asegurar codificación UTF-8 en consola de Windows
if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8')
if hasattr(sys.stderr, 'reconfigure'):
    sys.stderr.reconfigure(encoding='utf-8')

# Cargar variables de entorno
load_dotenv()
MONGO_URI = os.getenv("MONGO_URI")
DB_NAME = os.getenv("MONGO_DB_NAME", "hyperflix_db")


def banner():
    print("=" * 80)
    print("🎬 HYPERFLIX: CONSTRUCCIÓN DEL DATA WAREHOUSE (MODELO EN ESTRELLA - FASE 3)")
    print("   Dominio: Plataforma de Streaming de Video, Canales IPTV y Telemetría")
    print("   Arquitectura: reproducciones_analiticas ➔ Aggregation Pipelines ➔ Star Schema")
    print("=" * 80)


def construir_data_warehouse_hyperflix():
    banner()
    inicio_total = time.time()
    
    # -------------------------------------------------------------------------
    # PASO 1 y 2. Conexión a MongoDB Atlas
    # -------------------------------------------------------------------------
    print(f"\n🔄 PASO 1 & 2: CONECTANDO A MONGODB ATLAS (Base: '{DB_NAME}')...")
    client = pymongo.MongoClient(MONGO_URI, serverSelectionTimeoutMS=10000)
    db = client[DB_NAME]
    
    coleccion_fuente = "reproducciones_analiticas"
    if coleccion_fuente not in db.list_collection_names():
        print(f"  ⚠️ No se encontró la colección '{coleccion_fuente}' en '{DB_NAME}'.")
        print("  💡 Generando colección analítica con 'scripts/transformar_oltp_a_olap.py'...")
        from transformar_oltp_a_olap import transformar_oltp_a_olap_hyperflix
        transformar_oltp_a_olap_hyperflix()
        
    total_fuente = db[coleccion_fuente].count_documents({})
    print(f"  ✅ Conectado a Atlas exitosamente.")
    print(f"  📦 Colección fuente: '{coleccion_fuente}' con {total_fuente:,} eventos analíticos.\n")
    
    # -------------------------------------------------------------------------
    # PASO 3. Crear la Dimensión Tiempo (dim_tiempo)
    # -------------------------------------------------------------------------
    print("-" * 80)
    print("📅 PASO 3: CREANDO LA DIMENSIÓN TIEMPO (dim_tiempo)...")
    print("   - Agrupa por fecha, anio, mes, dia, hora y dia_semana para eliminar duplicados")
    print("   - Calcula 'trimestre' usando $ceil y $divide (1-4)")
    print("   - Determina 'es_fin_de_semana' usando $cond (Días 1 o 7)")
    print("   - Determina 'es_hora_pico' para análisis de tráfico (19:00 - 23:00)")
    print("   - Persiste directamente con $merge y allowDiskUse=True")
    print("-" * 80)
    
    db.dim_tiempo.drop()
    
    pipeline_tiempo = [
        {
            "$group": {
                "_id": {
                    "fecha": "$fecha_completa",
                    "anio": "$anio",
                    "mes": "$mes",
                    "dia": "$dia",
                    "hora": "$hora",
                    "dia_semana": "$dia_semana"
                }
            }
        },
        {
            "$project": {
                "_id": "$_id.fecha",
                "fecha": "$_id.fecha",
                "anio": "$_id.anio",
                "mes": "$_id.mes",
                "dia": "$_id.dia",
                "hora": "$_id.hora",
                "dia_semana": "$_id.dia_semana",
                "trimestre": {
                    "$ceil": {"$divide": ["$_id.mes", 3]}
                },
                "es_fin_de_semana": {
                    "$cond": {
                        "if": {
                            "$or": [
                                {"$eq": ["$_id.dia_semana", 1]},
                                {"$eq": ["$_id.dia_semana", 7]}
                            ]
                        },
                        "then": "Sí",
                        "else": "No"
                    }
                },
                "es_hora_pico": {
                    "$cond": {
                        "if": {
                            "$and": [
                                {"$gte": ["$_id.hora", 19]},
                                {"$lte": ["$_id.hora", 23]}
                            ]
                        },
                        "then": "Sí",
                        "else": "No"
                    }
                }
            }
        },
        {
            "$merge": {
                "into": "dim_tiempo",
                "whenMatched": "replace",
                "whenNotMatched": "insert"
            }
        }
    ]
    
    db[coleccion_fuente].aggregate(pipeline_tiempo, allowDiskUse=True)
    total_tiempo = db.dim_tiempo.count_documents({})
    print(f"  ✅ 'dim_tiempo' creada exitosamente: {total_tiempo:,} marcas temporales únicas.")
    
    # -------------------------------------------------------------------------
    # PASO 4. Crear la Dimensión Usuario (dim_usuario) [Equivalente a dim_cliente]
    # -------------------------------------------------------------------------
    print("\n" + "-" * 80)
    print("👤 PASO 4: CREANDO LA DIMENSIÓN USUARIO (dim_usuario)...")
    print("   - Agrupa por usuario.id")
    print("   - Usa $first para datos estáticos (codigo, nombre, pais, ciudad, plan, segmento)")
    print("   - Acumula métricas: total de sesiones ($sum), minutos totales vistos ($sum)")
    print("   - Calcula promedios con $avg y redondea con $round (minutos por sesión, latencia QoS)")
    print("   - Persiste con $merge y allowDiskUse=True")
    print("-" * 80)
    
    db.dim_usuario.drop()
    
    pipeline_usuario = [
        {
            "$group": {
                "_id": "$usuario.id",
                "codigo_usuario": {"$first": "$usuario.codigo"},
                "nombre": {"$first": "$usuario.nombre"},
                "pais": {"$first": "$usuario.pais"},
                "ciudad": {"$first": "$usuario.ciudad"},
                "plan_suscripcion": {"$first": "$usuario.plan"},
                "segmento": {"$first": "$usuario.segmento"},
                "total_sesiones": {"$sum": 1},
                "total_minutos_vistos_raw": {"$sum": "$duracion_vista_minutos"},
                "promedio_minutos_sesion_raw": {"$avg": "$duracion_vista_minutos"},
                "latencia_promedio_raw": {"$avg": "$latencia_ms"}
            }
        },
        {
            "$project": {
                "_id": 1,
                "usuario_id": "$_id",
                "codigo_usuario": 1,
                "nombre": 1,
                "pais": 1,
                "ciudad": 1,
                "plan_suscripcion": 1,
                "segmento": 1,
                "total_sesiones": 1,
                "total_minutos_vistos": {"$round": ["$total_minutos_vistos_raw", 1]},
                "promedio_minutos_sesion": {"$round": ["$promedio_minutos_sesion_raw", 1]},
                "latencia_promedio_ms": {"$round": ["$latencia_promedio_raw", 1]}
            }
        },
        {
            "$merge": {
                "into": "dim_usuario",
                "whenMatched": "replace",
                "whenNotMatched": "insert"
            }
        }
    ]
    
    db[coleccion_fuente].aggregate(pipeline_usuario, allowDiskUse=True)
    total_usuarios = db.dim_usuario.count_documents({})
    print(f"  ✅ 'dim_usuario' creada exitosamente: {total_usuarios:,} usuarios consolidados en el DW.")
    
    # -------------------------------------------------------------------------
    # PASO 5. Crear la Dimensión Contenido (dim_contenido) [Equivalente a dim_producto]
    # -------------------------------------------------------------------------
    print("\n" + "-" * 80)
    print("🎬 PASO 5: CREANDO LA DIMENSIÓN CONTENIDO (dim_contenido)...")
    print("   - Agrupa por contenido.id")
    print("   - Extrae con $first: título, tipo (Película / Canal IPTV), género o categoría")
    print("   - Acumula métricas: total reproducciones ($sum) y minutos reproducidos ($sum)")
    print("   - Calcula porcentaje medio de completitud ($avg + $round)")
    print("   - Persiste con $merge y allowDiskUse=True")
    print("-" * 80)
    
    db.dim_contenido.drop()
    
    pipeline_contenido = [
        {
            "$group": {
                "_id": "$contenido.id",
                "titulo": {"$first": "$contenido.titulo"},
                "tipo_contenido": {"$first": "$contenido.tipo"},
                "genero_o_categoria": {"$first": "$contenido.genero_o_categoria"},
                "anio_estreno": {"$first": "$contenido.anio"},
                "total_reproducciones": {"$sum": 1},
                "minutos_totales_raw": {"$sum": "$duracion_vista_minutos"},
                "porcentaje_completitud_raw": {"$avg": "$porcentaje_visto"},
                "bitrate_promedio_raw": {"$avg": "$bitrate_kbps"}
            }
        },
        {
            "$project": {
                "_id": 1,
                "contenido_id": "$_id",
                "titulo": 1,
                "tipo_contenido": 1,
                "genero_o_categoria": 1,
                "anio_estreno": 1,
                "total_reproducciones": 1,
                "minutos_totales": {"$round": ["$minutos_totales_raw", 1]},
                "porcentaje_completitud_promedio": {"$round": ["$porcentaje_completitud_raw", 1]},
                "bitrate_promedio_kbps": {"$round": ["$bitrate_promedio_raw", 0]}
            }
        },
        {
            "$merge": {
                "into": "dim_contenido",
                "whenMatched": "replace",
                "whenNotMatched": "insert"
            }
        }
    ]
    
    db[coleccion_fuente].aggregate(pipeline_contenido, allowDiskUse=True)
    total_contenido = db.dim_contenido.count_documents({})
    print(f"  ✅ 'dim_contenido' creada exitosamente: {total_contenido:,} títulos en catálogo dimensional.")
    
    # -------------------------------------------------------------------------
    # PASO 5B. Dimensión Dispositivo (dim_dispositivo) [Específica de Streaming]
    # -------------------------------------------------------------------------
    print("\n" + "-" * 80)
    print("📱 PASO 5B: CREANDO LA DIMENSIÓN DISPOSITIVO (dim_dispositivo)...")
    print("   - Agrupa por dispositivo.tipo (Smart TV, Móvil, PC, Tablet)")
    print("   - Evalúa Calidad de Servicio (QoS): latencia media y bitrate medio")
    print("-" * 80)
    
    db.dim_dispositivo.drop()
    
    pipeline_dispositivo = [
        {
            "$group": {
                "_id": "$dispositivo.tipo",
                "sistema_operativo": {"$first": "$dispositivo.sistema_operativo"},
                "resolucion_max": {"$first": "$dispositivo.resolucion"},
                "total_sesiones": {"$sum": 1},
                "latencia_promedio_raw": {"$avg": "$latencia_ms"},
                "bitrate_promedio_raw": {"$avg": "$bitrate_kbps"}
            }
        },
        {
            "$project": {
                "_id": 1,
                "tipo_dispositivo": "$_id",
                "sistema_operativo": 1,
                "resolucion_max": 1,
                "total_sesiones": 1,
                "latencia_promedio_ms": {"$round": ["$latencia_promedio_raw", 1]},
                "bitrate_promedio_kbps": {"$round": ["$bitrate_promedio_raw", 0]}
            }
        },
        {
            "$merge": {
                "into": "dim_dispositivo",
                "whenMatched": "replace",
                "whenNotMatched": "insert"
            }
        }
    ]
    
    db[coleccion_fuente].aggregate(pipeline_dispositivo, allowDiskUse=True)
    total_dispositivos = db.dim_dispositivo.count_documents({})
    print(f"  ✅ 'dim_dispositivo' creada exitosamente: {total_dispositivos:,} familias de dispositivos.")
    
    # -------------------------------------------------------------------------
    # PASO 6. Crear la Tabla de Hechos Central (fact_reproducciones) [Equivalente a fact_ventas]
    # -------------------------------------------------------------------------
    print("\n" + "-" * 80)
    print("⭐ PASO 6: CREANDO LA TABLA DE HECHOS CENTRAL (fact_reproducciones)...")
    print("   - Guarda únicamente claves foráneas a las dimensiones (fecha_id, usuario_id, contenido_id)")
    print("   - Conserva las medidas cuantitativas (duracion_segundos, % visto, bitrate, latencia)")
    print("   - Reduce duplicación masiva optimizando el almacenamiento dimensional")
    print("   - Persiste con $merge y allowDiskUse=True")
    print("-" * 80)
    
    db.fact_reproducciones.drop()
    
    pipeline_hechos = [
        {
            "$project": {
                "_id": 1,
                "codigo_evento": "$codigo_evento",
                
                # Claves Foráneas (Foreign Keys a Dimensiones)
                "fecha_id": "$fecha_completa",
                "usuario_id": "$usuario.id",
                "contenido_id": "$contenido.id",
                "dispositivo_tipo": "$dispositivo.tipo",
                
                # Medidas y Métricas Numéricas de Telemetría (Hechos Cuantitativos)
                "tipo_evento": "$tipo_evento",
                "duracion_segundos": "$duracion_vista_segundos",
                "duracion_minutos": "$duracion_vista_minutos",
                "porcentaje_visto": "$porcentaje_visto",
                "bitrate_kbps": "$bitrate_kbps",
                "latencia_ms": "$latencia_ms"
            }
        },
        {
            "$merge": {
                "into": "fact_reproducciones",
                "whenMatched": "replace",
                "whenNotMatched": "insert"
            }
        }
    ]
    
    db[coleccion_fuente].aggregate(pipeline_hechos, allowDiskUse=True)
    total_hechos = db.fact_reproducciones.count_documents({})
    print(f"  ✅ 'fact_reproducciones' creada exitosamente: {total_hechos:,} hechos cargados.")
    
    # -------------------------------------------------------------------------
    # PASO 7 & 8. Resumen Final de Auditoría del Data Warehouse
    # -------------------------------------------------------------------------
    duracion = time.time() - inicio_total
    print("\n" + "=" * 80)
    print("🏁 DATA WAREHOUSE HYPERFLIX CONFIGURADO EXITOSAMENTE (RESUMEN FINAL)")
    print("=" * 80)
    print(f"  Base de datos activa : {DB_NAME}")
    print(f"  Colección fuente     : {coleccion_fuente} ({total_fuente:,} registros originales)")
    print(f"  ⏱️ Tiempo total ETL   : {duracion:.2f} segundos")
    print("  ------------------------------------------------------------------------")
    print(f"  📅 dim_tiempo         : {db.dim_tiempo.count_documents({}):,} registros")
    print(f"  👤 dim_usuario        : {db.dim_usuario.count_documents({}):,} perfiles consolidados")
    print(f"  🎬 dim_contenido      : {db.dim_contenido.count_documents({}):,} títulos (VOD / IPTV)")
    print(f"  📱 dim_dispositivo    : {db.dim_dispositivo.count_documents({}):,} categorías QoS")
    print(f"  ⭐ fact_reproducciones: {db.fact_reproducciones.count_documents({}):,} hechos de telemetría")
    print("=" * 80)
    print("🎯 ¡Modelo Estrella de Streaming listo para BI, Dashboards y ClickHouse / Metabase!\n")
    
    client.close()
    return True


if __name__ == "__main__":
    construir_data_warehouse_hyperflix()
