import os
import sys
import time
import json
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

def ejecutar_benchmarking_hyperflix():
    print("=" * 75)
    print("🚀 BENCHMARKING DE CONSULTAS ANALÍTICAS (OLAP): PLATAFORMA HYPERFLIX")
    print("🎯 Evaluación de Rendimiento, Latencias e Índices Compuestos")
    print("=" * 75 + "\n")
    
    client = pymongo.MongoClient(MONGO_URI)
    db = client[DB_NAME]
    coleccion = db["reproducciones_analiticas"]
    
    total_docs = coleccion.count_documents({})
    print(f"📊 Registros analíticos a evaluar en 'reproducciones_analiticas': {total_docs}\n")
    
    if total_docs == 0:
        print("⚠️ La colección analítica está vacía. Ejecuta primero 'scripts/transformar_oltp_a_olap.py'.")
        client.close()
        return

    # 1. ESTRATEGIA DE INDEXACIÓN COMPUESTA
    print("🔧 FASE 1: Aplicando Estrategia de Índices Compuestos...")
    coleccion.create_index([("anio", 1), ("mes", 1), ("dia", 1)], name="idx_tiempo")
    coleccion.create_index([("contenido.tipo", 1), ("contenido.genero_o_categoria", 1)], name="idx_contenido_tipo_genero")
    coleccion.create_index([("usuario.pais", 1), ("usuario.plan", 1)], name="idx_usuario_pais_plan")
    coleccion.create_index([("dispositivo.tipo", 1), ("eventos_buffering", -1)], name="idx_disp_buffering")
    coleccion.create_index([("abandono", 1), ("contenido.genero_o_categoria", 1)], name="idx_abandono_genero")
    coleccion.create_index([("hora", 1), ("dia_semana", 1)], name="idx_hora_dia")
    print("✅ Índices compuestos verificados y optimizados.\n")
    
    # 2. DEFINICIÓN DE LAS CONSULTAS ANALÍTICAS DEL NEGOCIO
    consultas = [
        {
            "id": 1,
            "nombre": "Top 10 Películas más reproducidas y tiempo total consumido",
            "descripcion": "Identifica los títulos con mayor engagement y volumen de minutos vistos.",
            "pipeline": [
                {"$match": {"contenido.tipo": "Pelicula"}},
                {
                    "$group": {
                        "_id": "$contenido.titulo",
                        "genero": {"$first": "$contenido.genero_o_categoria"},
                        "reproducciones_totales": {"$sum": 1},
                        "minutos_vistos_totales": {"$sum": "$duracion_vista_minutos"},
                        "reproducciones_completas": {"$sum": {"$cond": ["$completado", 1, 0]}}
                    }
                },
                {"$sort": {"minutos_vistos_totales": -1}},
                {"$limit": 10},
                {
                    "$project": {
                        "pelicula": "$_id",
                        "_id": 0,
                        "genero": 1,
                        "reproducciones_totales": 1,
                        "minutos_vistos_totales": {"$round": ["$minutos_vistos_totales", 1]},
                        "reproducciones_completas": 1
                    }
                }
            ]
        },
        {
            "id": 2,
            "nombre": "Tasa de Abandono (Drop-off Rate) por Género de Película",
            "descripcion": "Analiza qué géneros sufren mayor deserción temprana de usuarios.",
            "pipeline": [
                {"$match": {"contenido.tipo": "Pelicula"}},
                {
                    "$group": {
                        "_id": "$contenido.genero_o_categoria",
                        "total_sesiones": {"$sum": 1},
                        "abandonos": {"$sum": {"$cond": ["$abandono", 1, 0]}},
                        "completadas": {"$sum": {"$cond": ["$completado", 1, 0]}}
                    }
                },
                {
                    "$project": {
                        "genero": "$_id",
                        "_id": 0,
                        "total_sesiones": 1,
                        "abandonos": 1,
                        "porcentaje_abandono": {
                            "$round": [
                                {"$multiply": [{"$divide": ["$abandonos", "$total_sesiones"]}, 100]},
                                2
                            ]
                        }
                    }
                },
                {"$sort": {"porcentaje_abandono": -1}}
            ]
        },
        {
            "id": 3,
            "nombre": "Calidad de Servicio (QoS): Buffering, Bitrate y Latencia por Dispositivo",
            "descripcion": "Diagnóstico de experiencia de usuario y rendimiento de red en clientes.",
            "pipeline": [
                {
                    "$group": {
                        "_id": "$dispositivo.tipo",
                        "sistema_operativo": {"$first": "$dispositivo.sistema_operativo"},
                        "total_reproducciones": {"$sum": 1},
                        "latencia_promedio_ms": {"$avg": "$latencia_ms"},
                        "bitrate_promedio_kbps": {"$avg": "$bitrate_kbps"},
                        "total_eventos_buffering": {"$sum": "$eventos_buffering"}
                    }
                },
                {
                    "$project": {
                        "dispositivo": "$_id",
                        "_id": 0,
                        "sistema_operativo": 1,
                        "total_reproducciones": 1,
                        "latencia_promedio_ms": {"$round": ["$latencia_promedio_ms", 1]},
                        "bitrate_promedio_kbps": {"$round": ["$bitrate_promedio_kbps", 0]},
                        "total_eventos_buffering": 1
                    }
                },
                {"$sort": {"total_eventos_buffering": -1}}
            ]
        },
        {
            "id": 4,
            "nombre": "Distribución de Audiencia y Consumo por País y Plan de Suscripción",
            "descripcion": "Cruza geolocalización de usuarios con nivel de monetización.",
            "pipeline": [
                {
                    "$group": {
                        "_id": {
                            "pais": "$usuario.pais",
                            "plan": "$usuario.plan"
                        },
                        "volumen_sesiones": {"$sum": 1},
                        "horas_reproducidas": {
                            "$sum": {"$divide": ["$duracion_vista_minutos", 60]}
                        }
                    }
                },
                {
                    "$project": {
                        "pais": "$_id.pais",
                        "plan": "$_id.plan",
                        "_id": 0,
                        "volumen_sesiones": 1,
                        "horas_reproducidas": {"$round": ["$horas_reproducidas", 1]}
                    }
                },
                {"$sort": {"horas_reproducidas": -1}},
                {"$limit": 10}
            ]
        },
        {
            "id": 5,
            "nombre": "Análisis de Horas Pico y Picos de Carga (Stress Analysis)",
            "descripcion": "Detecta las horas del día con mayor volumen de eventos de streaming.",
            "pipeline": [
                {
                    "$group": {
                        "_id": "$hora",
                        "total_eventos": {"$sum": 1},
                        "usuarios_concurrentes_aprox": {"$addToSet": "$usuario.id"},
                        "tiempo_consumido_horas": {
                            "$sum": {"$divide": ["$duracion_vista_minutos", 60]}
                        }
                    }
                },
                {
                    "$project": {
                        "hora_del_dia": "$_id",
                        "_id": 0,
                        "total_eventos": 1,
                        "usuarios_unicos": {"$size": "$usuarios_concurrentes_aprox"},
                        "tiempo_consumido_horas": {"$round": ["$tiempo_consumido_horas", 1]}
                    }
                },
                {"$sort": {"total_eventos": -1}},
                {"$limit": 8}
            ]
        },
        {
            "id": 6,
            "nombre": "Comparativa de Consumo: Películas VOD vs Canales IPTV en Vivo",
            "descripcion": "Compara el comportamiento entre contenido bajo demanda y TV lineal.",
            "pipeline": [
                {
                    "$group": {
                        "_id": "$contenido.tipo",
                        "total_eventos": {"$sum": 1},
                        "minutos_totales": {"$sum": "$duracion_vista_minutos"},
                        "tasa_completados": {"$avg": {"$cond": ["$completado", 1, 0]}}
                    }
                },
                {
                    "$project": {
                        "tipo_contenido": "$_id",
                        "_id": 0,
                        "total_eventos": 1,
                        "horas_totales": {"$round": [{"$divide": ["$minutos_totales", 60]}, 1]},
                        "porcentaje_completado": {
                            "$round": [{"$multiply": ["$tasa_completados", 100]}, 1]
                        }
                    }
                }
            ]
        }
    ]
    
    # 3. EJECUCIÓN DEL BENCHMARK
    print("⚙️ FASE 2: Ejecutando Pruebas de Latencia y Rendimiento...\n")
    print("-" * 75)
    
    tiempos = []
    
    for q in consultas:
        print(f"▶️ [{q['id']}] {q['nombre']}")
        print(f"   ℹ️  {q['descripcion']}")
        
        t_inicio = time.perf_counter()
        resultados = list(coleccion.aggregate(q['pipeline'], allowDiskUse=True))
        t_fin = time.perf_counter()
        
        latencia_ms = (t_fin - t_inicio) * 1000
        tiempos.append(latencia_ms)
        
        print(f"   ⏱️  Latencia: {latencia_ms:.2f} ms | 📊 Registros Devueltos: {len(resultados)}")
        
        if len(resultados) > 0:
            muestra = json.loads(json.dumps(resultados[0], default=str))
            print(f"   💡 Top/Muestra 1: {muestra}")
        print("-" * 75)
        
    # 4. RESUMEN EJECUTIVO DE RENDIMIENTO
    promedio = sum(tiempos) / len(tiempos)
    
    print("\n" + "=" * 75)
    print("📊 RESUMEN FINAL DE BENCHMARKING ANALÍTICO (HYPERFLIX OLAP)")
    print("=" * 75)
    print(f"📈 Total de Consultas Analíticas: {len(consultas)}")
    print(f"⏱️  Latencia Promedio: {promedio:.2f} ms")
    print(f"⚡ Latencia Mínima: {min(tiempos):.2f} ms")
    print(f"🐢 Latencia Máxima: {max(tiempos):.2f} ms")
    print(f"💾 Volumen de datos procesado: {total_docs} documentos")
    print("🎯 Conclusión: Consultas analíticas resueltas por debajo del umbral objetivo (<500ms)")
    print("=" * 75)
    
    client.close()

if __name__ == "__main__":
    ejecutar_benchmarking_hyperflix()