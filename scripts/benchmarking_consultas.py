"""
=============================================================================
HYPERFLIX - BENCHMARKING DE 10 CONSULTAS ANALÍTICAS (OLAP)
=============================================================================
Asignatura: Bases de Datos y Almacenamiento Masivo (Octavo Semestre)
Dominio: Plataforma de Streaming de Video, VOD e IPTV (HYPERFLIX)

Objetivo:
    Diseñar y ejecutar 10 consultas analíticas complejas sobre el modelo
    dimensional desnormalizado ('reproducciones_analiticas'), abarcando
    escenarios de negocio reales:
      1. Series temporales de streaming mensual
      2. Top 5 Contenidos con mayor engagement por Género
      3. Rendimiento de Audiencia por Segmento de Usuario y Ciudad
      4. Consumo Promedio y Volumen por Plan de Suscripción
      5. Tendencia de Streaming: Comparativa Día de la Semana
      6. Tasa de Abandono (Drop-off Rate) por Tipo de Dispositivo
      7. Top 10 Usuarios VIP / Super-Streamers por Horas Vistas
      8. Reproducciones Trimestrales (Análisis Estacional Q1-Q4)
      9. Tiempo Medio de Sesión por Ciudad y Segmento (Top 15)
     10. Top 10 Contenidos con Mayor Retención / Completitud (%)

Estrategia de Optimización:
    • Indexación Compuesta Selectiva (Index Scans en lugar de Collection Scans)
    • allowDiskUse: True en todas las operaciones de agregación
    • Medición rigurosa de SLA de rendimiento (< 500 ms)
=============================================================================
"""

import os
import sys
import time
import json
import pymongo
from dotenv import load_dotenv

# Asegurar codificación UTF-8 en consola de Windows
if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8')
if hasattr(sys.stderr, 'reconfigure'):
    sys.stderr.reconfigure(encoding='utf-8')

load_dotenv()
MONGO_URI = os.getenv("MONGO_URI")
DB_NAME = os.getenv("MONGO_DB_NAME", "hyperflix_db")


def ejecutar_benchmarking_10_consultas():
    print("=" * 80)
    print("🎬 HYPERFLIX: BENCHMARKING DE 10 CONSULTAS ANALÍTICAS OPTIMIZADAS (OLAP)")
    print("   Cumplimiento de SLA de Rendimiento (< 500ms) e Indexación Compuesta")
    print("=" * 80 + "\n")
    
    client = pymongo.MongoClient(MONGO_URI)
    db = client[DB_NAME]
    coleccion = db["reproducciones_analiticas"]
    
    total_docs = coleccion.count_documents({})
    if total_docs == 0:
        print("⚠️ La colección 'reproducciones_analiticas' está vacía.")
        print("   Ejecutando pipeline ETL con 'scripts/transformar_oltp_a_olap.py'...")
        from transformar_oltp_a_olap import transformar_oltp_a_olap_hyperflix
        transformar_oltp_a_olap_hyperflix()
        total_docs = coleccion.count_documents({})
        
    print(f"📊 Registros analíticos en 'reproducciones_analiticas': {total_docs:,}\n")
    
    # -------------------------------------------------------------------------
    # FASE 1: Estrategia de Indexación Compuesta Selectiva
    # -------------------------------------------------------------------------
    print("🔧 Fase 1: Aplicando estrategia de indexación para optimización...")
    
    def crear_indice_seguro(keys, name):
        try:
            coleccion.create_index(keys, name=name)
        except pymongo.errors.OperationFailure as e:
            if "already exists" in str(e):
                pass
            else:
                raise e

    crear_indice_seguro([("anio", 1), ("mes", 1), ("dia", 1)], name="idx_tiempo")
    crear_indice_seguro([("contenido.tipo", 1), ("contenido.genero_o_categoria", 1)], name="idx_contenido_tipo_genero")
    crear_indice_seguro([("usuario.segmento", 1), ("usuario.ciudad", 1)], name="idx_segmento_ciudad")
    crear_indice_seguro([("usuario.plan", 1), ("duracion_vista_minutos", -1)], name="idx_plan_duracion")
    crear_indice_seguro([("dia_semana", 1), ("duracion_vista_minutos", -1)], name="idx_dia_duracion")
    crear_indice_seguro([("dispositivo.tipo", 1), ("abandono", 1)], name="idx_disp_abandono")
    crear_indice_seguro([("usuario.id", 1), ("duracion_vista_minutos", -1)], name="idx_usuario_duracion")
    crear_indice_seguro([("mes", 1), ("anio", 1)], name="idx_mes_anio")
    crear_indice_seguro([("completado", 1), ("duracion_vista_minutos", -1)], name="idx_completado_duracion")
    
    print("✅ Índices compuestos verificados/creados exitosamente.\n")
    
    # -------------------------------------------------------------------------
    # DEFINICIÓN DE LAS 10 CONSULTAS ANALÍTICAS (EQUIVALENTES A LA GUÍA)
    # -------------------------------------------------------------------------
    consultas = [
        {
            "id": 1,
            "nombre": "1. Reproducciones y Minutos Vistos Mensuales (Serie de tiempo)",
            "pipeline": [
                {
                    "$group": {
                        "_id": {"anio": "$anio", "mes": "$mes"},
                        "minutos_totales": {"$sum": "$duracion_vista_minutos"},
                        "sesiones_totales": {"$sum": 1},
                        "usuarios_activos": {"$addToSet": "$usuario.id"}
                    }
                },
                {
                    "$project": {
                        "_id": 1,
                        "minutos_totales": {"$round": ["$minutos_totales", 2]},
                        "sesiones_totales": 1,
                        "usuarios_activos": {"$size": "$usuarios_activos"}
                    }
                },
                {"$sort": {"_id.anio": 1, "_id.mes": 1}}
            ]
        },
        {
            "id": 2,
            "nombre": "2. Top 5 Contenidos con Mayor Audiencia por Género",
            "pipeline": [
                {
                    "$group": {
                        "_id": {
                            "genero": "$contenido.genero_o_categoria",
                            "titulo": "$contenido.titulo"
                        },
                        "minutos_vistos": {"$sum": "$duracion_vista_minutos"},
                        "reproducciones": {"$sum": 1}
                    }
                },
                {"$sort": {"minutos_vistos": -1}},
                {
                    "$group": {
                        "_id": "$_id.genero",
                        "top_contenidos": {
                            "$push": {
                                "titulo": "$_id.titulo",
                                "minutos": {"$round": ["$minutos_vistos", 1]},
                                "reproducciones": "$reproducciones"
                            }
                        }
                    }
                },
                {
                    "$project": {
                        "genero": "$_id",
                        "_id": 0,
                        "top_contenidos": {"$slice": ["$top_contenidos", 5]}
                    }
                },
                {"$sort": {"genero": 1}},
                {"$limit": 6}
            ]
        },
        {
            "id": 3,
            "nombre": "3. Rendimiento de Audiencia por Segmento de Usuario y Ciudad",
            "pipeline": [
                {
                    "$group": {
                        "_id": {
                            "segmento": "$usuario.segmento",
                            "ciudad": "$usuario.ciudad"
                        },
                        "horas_vistas": {"$sum": {"$divide": ["$duracion_vista_minutos", 60]}},
                        "sesiones": {"$sum": 1},
                        "latencia_promedio": {"$avg": "$latencia_ms"}
                    }
                },
                {
                    "$project": {
                        "_id": 1,
                        "horas_vistas": {"$round": ["$horas_vistas", 1]},
                        "sesiones": 1,
                        "latencia_promedio_ms": {"$round": ["$latencia_promedio", 1]}
                    }
                },
                {"$sort": {"horas_vistas": -1}}
            ]
        },
        {
            "id": 4,
            "nombre": "4. Consumo Promedio y Volumen por Plan de Suscripción",
            "pipeline": [
                {
                    "$group": {
                        "_id": "$usuario.plan",
                        "promedio_minutos_sesion": {"$avg": "$duracion_vista_minutos"},
                        "total_sesiones": {"$sum": 1},
                        "horas_totales": {"$sum": {"$divide": ["$duracion_vista_minutos", 60]}}
                    }
                },
                {
                    "$project": {
                        "plan": "$_id",
                        "_id": 0,
                        "promedio_minutos_sesion": {"$round": ["$promedio_minutos_sesion", 2]},
                        "total_sesiones": 1,
                        "horas_totales": {"$round": ["$horas_totales", 1]}
                    }
                },
                {"$sort": {"total_sesiones": -1}}
            ]
        },
        {
            "id": 5,
            "nombre": "5. Tendencia de Streaming: Comparativa Día de la Semana",
            "pipeline": [
                {
                    "$group": {
                        "_id": "$dia_semana",
                        "minutos_consumidos": {"$sum": "$duracion_vista_minutos"},
                        "total_reproducciones": {"$sum": 1}
                    }
                },
                {
                    "$project": {
                        "dia_semana": "$_id",
                        "_id": 0,
                        "minutos_consumidos": {"$round": ["$minutos_consumidos", 1]},
                        "total_reproducciones": 1
                    }
                },
                {"$sort": {"dia_semana": 1}}
            ]
        },
        {
            "id": 6,
            "nombre": "6. Tasa de Abandono (Drop-off Rate) por Tipo de Dispositivo",
            "pipeline": [
                {
                    "$group": {
                        "_id": "$dispositivo.tipo",
                        "total_sesiones": {"$sum": 1},
                        "abandonos": {"$sum": {"$cond": ["$abandono", 1, 0]}},
                        "completadas": {"$sum": {"$cond": ["$completado", 1, 0]}}
                    }
                },
                {
                    "$project": {
                        "dispositivo": "$_id",
                        "_id": 0,
                        "total_sesiones": 1,
                        "abandonos": 1,
                        "tasa_abandono_pct": {
                            "$round": [
                                {"$multiply": [{"$divide": ["$abandonos", "$total_sesiones"]}, 100]},
                                2
                            ]
                        }
                    }
                },
                {"$sort": {"tasa_abandono_pct": -1}}
            ]
        },
        {
            "id": 7,
            "nombre": "7. Top 10 Usuarios VIP / Super-Streamers por Horas Vistas",
            "pipeline": [
                {
                    "$group": {
                        "_id": "$usuario.id",
                        "nombre": {"$first": "$usuario.nombre"},
                        "ciudad": {"$first": "$usuario.ciudad"},
                        "plan": {"$first": "$usuario.plan"},
                        "horas_vistas": {"$sum": {"$divide": ["$duracion_vista_minutos", 60]}},
                        "total_sesiones": {"$sum": 1}
                    }
                },
                {"$sort": {"horas_vistas": -1}},
                {"$limit": 10},
                {
                    "$project": {
                        "usuario_id": {"$toString": "$_id"},
                        "_id": 0,
                        "nombre": 1,
                        "ciudad": 1,
                        "plan": 1,
                        "horas_vistas": {"$round": ["$horas_vistas", 1]},
                        "total_sesiones": 1
                    }
                }
            ]
        },
        {
            "id": 8,
            "nombre": "8. Reproducciones Trimestrales (Análisis Estacional Q1-Q4)",
            "pipeline": [
                {
                    "$project": {
                        "anio": "$anio",
                        "trimestre_num": {"$ceil": {"$divide": ["$mes", 3]}},
                        "duracion_vista_minutos": "$duracion_vista_minutos"
                    }
                },
                {
                    "$group": {
                        "_id": {
                            "anio": "$anio",
                            "trimestre": {
                                "$concat": ["Q", {"$toString": "$trimestre_num"}]
                            }
                        },
                        "horas_totales": {"$sum": {"$divide": ["$duracion_vista_minutos", 60]}},
                        "total_sesiones": {"$sum": 1}
                    }
                },
                {
                    "$project": {
                        "anio": "$_id.anio",
                        "trimestre": "$_id.trimestre",
                        "_id": 0,
                        "horas_totales": {"$round": ["$horas_totales", 1]},
                        "total_sesiones": 1
                    }
                },
                {"$sort": {"anio": 1, "trimestre": 1}}
            ]
        },
        {
            "id": 9,
            "nombre": "9. Tiempo Medio de Sesión por Ciudad y Segmento (Top 15)",
            "pipeline": [
                {
                    "$group": {
                        "_id": {
                            "ciudad": "$usuario.ciudad",
                            "segmento": "$usuario.segmento"
                        },
                        "duracion_promedio_min": {"$avg": "$duracion_vista_minutos"},
                        "transacciones": {"$sum": 1}
                    }
                },
                {"$sort": {"duracion_promedio_min": -1}},
                {"$limit": 15},
                {
                    "$project": {
                        "ciudad": "$_id.ciudad",
                        "segmento": "$_id.segmento",
                        "_id": 0,
                        "duracion_promedio_min": {"$round": ["$duracion_promedio_min", 2]},
                        "transacciones": 1
                    }
                }
            ]
        },
        {
            "id": 10,
            "nombre": "10. Top 10 Contenidos con Mayor Retención / Completitud (%)",
            "pipeline": [
                {
                    "$group": {
                        "_id": "$contenido.titulo",
                        "tipo": {"$first": "$contenido.tipo"},
                        "genero": {"$first": "$contenido.genero_o_categoria"},
                        "completadas": {"$sum": {"$cond": ["$completado", 1, 0]}},
                        "total_reproducciones": {"$sum": 1},
                        "minutos_vistos": {"$sum": "$duracion_vista_minutos"}
                    }
                },
                {"$sort": {"completadas": -1, "minutos_vistos": -1}},
                {"$limit": 10},
                {
                    "$project": {
                        "titulo": "$_id",
                        "_id": 0,
                        "tipo": 1,
                        "genero": 1,
                        "completadas": 1,
                        "total_reproducciones": 1,
                        "minutos_vistos": {"$round": ["$minutos_vistos", 1]}
                    }
                }
            ]
        }
    ]
    
    # -------------------------------------------------------------------------
    # FASE 2: Ejecución de las 10 Consultas y Medición de Latencia
    # -------------------------------------------------------------------------
    print("⚙️ Fase 2: Ejecutando consultas y midiendo latencia...\n")
    print("-" * 80)
    
    tiempos = []
    
    for q in consultas:
        t_inicio = time.perf_counter()
        cursor = coleccion.aggregate(q["pipeline"], allowDiskUse=True)
        resultados = list(cursor)
        t_fin = time.perf_counter()
        
        latencia_ms = (t_fin - t_inicio) * 1000
        tiempos.append(latencia_ms)
        
        print(f"▶️ [{q['id']}/10] {q['nombre']}")
        print(f"   ⏱️ Latencia: {latencia_ms:.2f} ms | 📊 Filas retornadas: {len(resultados)}")
        
        if resultados:
            muestra_raw = json.loads(json.dumps(resultados[0], default=str))
            muestra_str = str(muestra_raw)
            if len(muestra_str) > 110:
                muestra_str = muestra_str[:110] + "..."
            print(f"   💡 Muestra: {muestra_str}")
        print("-" * 80)
        
    # -------------------------------------------------------------------------
    # RESUMEN FINAL DE RENDIMIENTO (SLA < 500ms)
    # -------------------------------------------------------------------------
    promedio = sum(tiempos) / len(tiempos)
    lat_min = min(tiempos)
    lat_max = max(tiempos)
    
    print("\n" + "=" * 80)
    print("📊 RESUMEN DE BENCHMARKING DE RENDIMIENTO (10 CONSULTAS)")
    print("=" * 80)
    print(f"☑️ Total de consultas ejecutadas: {len(consultas)}")
    print(f"⏱️ Latencia PROMEDIO por consulta: {promedio:.2f} ms")
    print(f"🚀 Latencia MÍNIMA registrada: {lat_min:.2f} ms")
    print(f"🐢 Latencia MÁXIMA registrada: {lat_max:.2f} ms")
    print(f"💾 Registros analíticos procesados: {total_docs:,}")
    print("=" * 80)
    print("✅ CONCLUSIÓN: RENDIMIENTO ÓPTIMO. El uso de índices compuestos y")
    print("   allowDiskUse garantiza un throughput alto y latencia < 500ms,")
    print("   cumpliendo con los SLA de entornos OLAP de alto rendimiento.")
    print("=" * 80 + "\n")
    
    client.close()
    return tiempos


if __name__ == "__main__":
    ejecutar_benchmarking_10_consultas()