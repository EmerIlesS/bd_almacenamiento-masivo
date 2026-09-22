"""
=============================================================================
FASE 3: CONSTRUCCIÓN DEL DATA WAREHOUSE (MODELO EN ESTRELLA / STAR SCHEMA)
=============================================================================
Asignatura: Bases de Datos y Almacenamiento Masivo (Octavo Semestre)
Objetivo:
    Transformar datos operacionales/enriquecidos (OLTP) de la colección
    'ventas_analiticas' en un Modelo en Estrella (OLAP) compuesto por:
      - dim_tiempo      (Calendario, Año, Mes, Trimestre, Fin de semana)
      - dim_cliente     (Perfiles únicos, Totales, Ticket promedio)
      - dim_producto    (Catálogo, Categorías, Unidades vendidas, Ingresos)
      - fact_ventas     (Tabla de hechos central con medidas cuantitativas)

Operadores y Técnicas Evaluadas:
    - Agregaciones: $group, $project, $merge, $ceil, $cond, $first, $sum, $avg, $round
    - Optimización Big Data: allowDiskUse=True
    - Verificación y auditoría: count_documents({})
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

# -----------------------------------------------------------------------------
# PASO 1. Cargar Variables de Entorno (.env)
# -----------------------------------------------------------------------------
load_dotenv()
MONGO_URI = os.getenv("MONGO_URI")
DB_NAME = os.getenv("MONGO_VENTAS_DB", "ventas_db")


def banner():
    print("=" * 78)
    print("🏛️  FASE 3: CONSTRUCCIÓN DEL DATA WAREHOUSE (MODELO EN ESTRELLA)")
    print("   Arquitectura: Colección Enriquecida ➔ Aggregation Pipelines ➔ Star Schema")
    print("=" * 78)


def construir_data_warehouse_estrella():
    banner()
    inicio_total = time.time()
    
    # -------------------------------------------------------------------------
    # PASO 2. Crear la Conexión a MongoDB Atlas
    # -------------------------------------------------------------------------
    print(f"\n🔄 PASO 2: CONECTANDO A MONGODB ATLAS (Base de Datos: '{DB_NAME}')...")
    client = pymongo.MongoClient(MONGO_URI, serverSelectionTimeoutMS=10000)
    db = client[DB_NAME]
    
    # Verificar existencia de la colección fuente
    colecciones_existentes = db.list_collection_names()
    coleccion_fuente = "ventas_analiticas"
    
    if coleccion_fuente not in colecciones_existentes:
        print(f"  ⚠️ La colección '{coleccion_fuente}' no existe en '{DB_NAME}'.")
        print(f"  📁 Colecciones detectadas: {colecciones_existentes}")
        client.close()
        return False
        
    total_origen = db[coleccion_fuente].count_documents({})
    print(f"  ✅ Conectado a Atlas exitosamente.")
    print(f"  📦 Colección fuente: '{coleccion_fuente}' con {total_origen:,} registros.\n")
    
    # -------------------------------------------------------------------------
    # PASO 3. Crear la Dimensión Tiempo (dim_tiempo)
    # -------------------------------------------------------------------------
    print("-" * 78)
    print("📅 PASO 3: CREANDO LA DIMENSIÓN TIEMPO (dim_tiempo)...")
    print("   - Elimina duplicados agrupando por fecha, anio, mes, dia")
    print("   - Calcula 'trimestre' usando $ceil y $divide")
    print("   - Determina 'es_fin_de_semana' usando condicional $cond")
    print("   - Persiste directamente con $merge y allowDiskUse=True")
    print("-" * 78)
    
    # Limpieza inicial para empezar desde cero
    db.dim_tiempo.drop()
    
    pipeline_tiempo = [
        # Etapa $group: Agrupa por fecha para eliminar duplicados
        {
            "$group": {
                "_id": {
                    "fecha": "$fecha_venta",
                    "anio": "$anio",
                    "mes": "$mes",
                    "dia": "$dia"
                }
            }
        },
        # Etapa $project: Calcula trimestre ($ceil) y fin de semana ($cond)
        {
            "$project": {
                "_id": "$_id.fecha",
                "fecha": "$_id.fecha",
                "anio": "$_id.anio",
                "mes": "$_id.mes",
                "dia": "$_id.dia",
                "trimestre": {
                    "$ceil": {
                        "$divide": ["$_id.mes", 3]
                    }
                },
                "es_fin_de_semana": {
                    "$cond": {
                        "if": {
                            "$or": [
                                {"$eq": ["$_id.dia", 1]},
                                {"$eq": ["$_id.dia", 7]}
                            ]
                        },
                        "then": "Sí",
                        "else": "No"
                    }
                }
            }
        },
        # Etapa $merge: Guarda o reemplaza en dim_tiempo
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
    print(f"  ✅ 'dim_tiempo' construida exitosamente: {total_tiempo:,} fechas únicas registradas.")
    
    # -------------------------------------------------------------------------
    # PASO 4. Crear la Dimensión Cliente (dim_cliente)
    # -------------------------------------------------------------------------
    print("\n" + "-" * 78)
    print("👤 PASO 4: CREANDO LA DIMENSIÓN CLIENTE (dim_cliente)...")
    print("   - Agrupa por cliente.id")
    print("   - Usa $first para tomar la primera ocurrencia de atributos estáticos (nombre, ciudad)")
    print("   - Calcula acumulados con $sum (total_compras)")
    print("   - Calcula métricas promedio con $avg y redondeo con $round (ticket_promedio)")
    print("   - Persiste con $merge y allowDiskUse=True")
    print("-" * 78)
    
    db.dim_cliente.drop()
    
    pipeline_cliente = [
        {
            "$group": {
                "_id": "$cliente.id",
                "nombre": {"$first": "$cliente.nombre"},
                "ciudad": {"$first": "$cliente.ciudad"},
                "segmento": {"$first": "$cliente.segmento"},
                "total_compras": {"$sum": "$total_venta"},
                "ticket_promedio_raw": {"$avg": "$total_venta"},
                "total_transacciones": {"$sum": 1}
            }
        },
        {
            "$project": {
                "_id": 1,
                "cliente_id": "$_id",
                "nombre": 1,
                "ciudad": 1,
                "segmento": 1,
                "total_compras": 1,
                "ticket_promedio": {"$round": ["$ticket_promedio_raw", 2]},
                "total_transacciones": 1
            }
        },
        {
            "$merge": {
                "into": "dim_cliente",
                "whenMatched": "replace",
                "whenNotMatched": "insert"
            }
        }
    ]
    
    db[coleccion_fuente].aggregate(pipeline_cliente, allowDiskUse=True)
    total_clientes = db.dim_cliente.count_documents({})
    print(f"  ✅ 'dim_cliente' construida exitosamente: {total_clientes:,} clientes consolidados.")
    
    # -------------------------------------------------------------------------
    # PASO 5. Crear la Dimensión Producto (dim_producto)
    # -------------------------------------------------------------------------
    print("\n" + "-" * 78)
    print("📦 PASO 5: CREANDO LA DIMENSIÓN PRODUCTO (dim_producto)...")
    print("   - Agrupa por producto.id")
    print("   - Toma atributos fijos con $first (nombre, categoría, marca)")
    print("   - Acumula unidades vendidas con $sum ($cantidad)")
    print("   - Acumula ingresos totales con $sum ($total_venta)")
    print("   - Persiste con $merge y allowDiskUse=True")
    print("-" * 78)
    
    db.dim_producto.drop()
    
    pipeline_producto = [
        {
            "$group": {
                "_id": "$producto.id",
                "nombre_producto": {"$first": "$producto.nombre"},
                "categoria": {"$first": "$producto.categoria"},
                "marca": {"$first": "$producto.marca"},
                "unidades_vendidas": {"$sum": "$cantidad"},
                "ingresos_totales": {"$sum": "$total_venta"},
                "precio_promedio_raw": {"$avg": "$precio_unitario"}
            }
        },
        {
            "$project": {
                "_id": 1,
                "producto_id": "$_id",
                "nombre_producto": 1,
                "categoria": 1,
                "marca": 1,
                "unidades_vendidas": 1,
                "ingresos_totales": 1,
                "precio_promedio": {"$round": ["$precio_promedio_raw", 2]}
            }
        },
        {
            "$merge": {
                "into": "dim_producto",
                "whenMatched": "replace",
                "whenNotMatched": "insert"
            }
        }
    ]
    
    db[coleccion_fuente].aggregate(pipeline_producto, allowDiskUse=True)
    total_productos = db.dim_producto.count_documents({})
    print(f"  ✅ 'dim_producto' construida exitosamente: {total_productos:,} productos en catálogo dimensional.")
    
    # -------------------------------------------------------------------------
    # PASO 6. Crear la Tabla de Hechos (fact_ventas)
    # -------------------------------------------------------------------------
    print("\n" + "-" * 78)
    print("⭐ PASO 6: CREANDO LA TABLA DE HECHOS CENTRAL (fact_ventas)...")
    print("   - Conserva únicamente las claves foráneas (fecha_id, cliente_id, producto_id)")
    print("   - Mantiene las medidas numéricas transaccionales (cantidad, precio, total)")
    print("   - Elimina la duplicación de datos descriptivos, optimizando el DW")
    print("   - Persiste con $merge y allowDiskUse=True")
    print("-" * 78)
    
    db.fact_ventas.drop()
    
    pipeline_hechos = [
        {
            "$project": {
                "_id": 1,
                "venta_id": {"$ifNull": ["$numero_factura", {"$toString": "$_id"}]},
                "fecha_id": "$fecha_venta",
                "cliente_id": "$cliente.id",
                "producto_id": "$producto.id",
                "cantidad": "$cantidad",
                "precio_unitario": "$precio_unitario",
                "total_venta": "$total_venta",
                "ganancia_estimada": "$ganancia_estimada",
                "metodo_pago": "$metodo_pago",
                "estado": "$estado"
            }
        },
        {
            "$merge": {
                "into": "fact_ventas",
                "whenMatched": "replace",
                "whenNotMatched": "insert"
            }
        }
    ]
    
    db[coleccion_fuente].aggregate(pipeline_hechos, allowDiskUse=True)
    total_hechos = db.fact_ventas.count_documents({})
    print(f"  ✅ 'fact_ventas' construida exitosamente: {total_hechos:,} transacciones de hechos cargadas.")
    
    # -------------------------------------------------------------------------
    # PASO 7 y 8. Resumen Final de Auditoría del Data Warehouse
    # -------------------------------------------------------------------------
    tiempo_total = time.time() - inicio_total
    print("\n" + "=" * 78)
    print("🏁 DATA WAREHOUSE CONFIGURADO (RESUMEN FINAL)")
    print("=" * 78)
    print(f"  Base de datos activa: {DB_NAME}")
    print(f"  ⏱️ Tiempo total de procesamiento ETL: {tiempo_total:.2f} segundos")
    print("  ------------------------------------------------------------------------")
    print(f"  📅 dim_tiempo     : {db.dim_tiempo.count_documents({}):,} registros")
    print(f"  👤 dim_cliente    : {db.dim_cliente.count_documents({}):,} registros")
    print(f"  📦 dim_producto   : {db.dim_producto.count_documents({}):,} registros")
    print(f"  ⭐ fact_ventas    : {db.fact_ventas.count_documents({}):,} hechos transaccionales")
    print("=" * 78)
    print("🎯 ¡Modelo Estrella listo para consumo por herramientas BI, SQL y Dashboards!\n")
    
    client.close()
    return True


if __name__ == "__main__":
    construir_data_warehouse_estrella()
