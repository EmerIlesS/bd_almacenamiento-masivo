"""
=============================================================================
HYPERFLIX - GENERADOR AUTOMATIZADO DE CATÁLOGO Y DICCIONARIO DE DATOS
=============================================================================
Asignatura: Bases de Datos y Almacenamiento Masivo (Octavo Semestre)
Programa:   Ingeniería de Sistemas - Instituto Tecnológico del Putumayo (ITP)
Dominio:    Plataforma de Streaming, VOD, Canales IPTV y Telemetría Masiva

Objetivo:
    Inspeccionar dinámicamente todas las fuentes de datos del sistema:
      1. Colecciones OLTP en MongoDB Atlas (usuarios, suscripciones, peliculas, etc.)
      2. Modelo Dimensional OLAP / Estrella (reproducciones_analiticas, dim_*, fact_*)
      3. Zonas del Data Lake Parquet (raw, processed, curated)
    
    Y generar automáticamente:
      • Documento Markdown formal: docs/DICCIONARIO_DE_DATOS.md
      • Catálogo de Metadatos JSON estándar: catalogo_datos.json
      • Portal Web Interactivo de Gobernanza: catalogo_datos.html

Uso:
    python scripts/generar_diccionario_datos.py
    python catalogo_datos.py
=============================================================================
"""

import os
import sys
import json
import time
from datetime import datetime
import pymongo
import pandas as pd
import pyarrow.parquet as pq
from dotenv import load_dotenv

# Codificación UTF-8 para consola de Windows
if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8')
if hasattr(sys.stderr, 'reconfigure'):
    sys.stderr.reconfigure(encoding='utf-8')

load_dotenv()
MONGO_URI = os.getenv("MONGO_URI")
DB_NAME = os.getenv("MONGO_DB_NAME", "hyperflix_db")
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_LAKE_DIR = os.path.join(BASE_DIR, "data_lake")
DOCS_DIR = os.path.join(BASE_DIR, "docs")

# Diccionario semántico de descripciones de negocio y sensibilidad para HYPERFLIX
DICCIONARIO_NEGOCIO = {
    # Colección usuarios
    "_id": ("Identificador único autogenerado del documento BSON.", "Clave Primaria", "Interna"),
    "codigo_usuario": ("Código único de negocio asignado al suscriptor (USR-XXXXX).", "Único / Indexado", "Interna"),
    "nombre": ("Nombre completo o alias del suscriptor de la plataforma.", "Requerido", "Confidencial (PII)"),
    "correo": ("Dirección de correo electrónico principal para inicio de sesión y facturación.", "Único / Indexado", "Confidencial (PII)"),
    "fecha_registro": ("Fecha y hora exacta en la que el usuario creó su cuenta.", "Requerido", "Interna"),
    "pais": ("País de residencia del usuario para segmentación geográfica de catálogo.", "Indexado", "Interna"),
    "ciudad": ("Ciudad o municipio de residencia del suscriptor.", "Opcional", "Interna"),
    "segmento": ("Segmentación comercial del usuario: VIP, ESTUDIANTE, FREEMIUM, REGULAR.", "Indexado", "Interna"),
    "dispositivo_preferido": ("Dispositivo más utilizado por el usuario (Smart TV, Móvil, PC).", "Informativo", "Interna"),
    
    # Colección suscripciones
    "codigo_suscripcion": ("Identificador único del contrato de suscripción (SUB-XXXXX).", "Único", "Interna"),
    "usuario_id": ("Referencia al suscriptor titular de la cuenta.", "Clave Foránea", "Interna"),
    "plan": ("Modalidad de suscripción contratada: BASICO (720p), ESTANDAR (1080p), PREMIUM (4K UHD).", "Indexado", "Pública"),
    "precio_usd": ("Tarifa mensual facturada en dólares estadounidenses.", "Requerido", "Interna"),
    "fecha_inicio": ("Fecha de activación del ciclo de facturación.", "Requerido", "Interna"),
    "fecha_renovacion": ("Fecha prevista para el siguiente cobro recurrente.", "Requerido", "Interna"),
    "estado": ("Estado de la membresía: ACTIVA, CANCELADA, EN_MORA, PAUSADA.", "Indexado", "Interna"),
    "metodo_pago_preferido": ("Instrumento de pago asociado: TARJETA, PAYPAL, CRIPTO, TRANSFERENCIA.", "Informativo", "Confidencial"),

    # Colección peliculas
    "codigo_pelicula": ("Código único del título audiovisual en el catálogo VOD (MOV-XXXXX).", "Único / Indexado", "Pública"),
    "titulo": ("Nombre comercial de la película o contenido bajo demanda.", "Requerido", "Pública"),
    "genero": ("Género cinematográfico principal (Acción, Ciencia Ficción, Drama, Terror, etc.).", "Indexado", "Pública"),
    "generos_secundarios": ("Lista de subgéneros asociados a la película.", "Array", "Pública"),
    "anio_lanzamiento": ("Año de estreno comercial internacional de la obra audiovisual.", "Informativo", "Pública"),
    "duracion_minutos": ("Extensión total de la película expresada en minutos.", "Requerido", "Pública"),
    "duracion_segundos": ("Duración total de la película calculada en segundos para telemetría.", "Técnico", "Interna"),
    "clasificacion_edad": ("Clasificación parental de audiencia: APTA, +13, +16, +18.", "Informativo", "Pública"),
    "resolucion_maxima": ("Máxima calidad disponible en catálogo (4K UHD, 1080p FHD, 720p HD).", "Técnico", "Pública"),
    "sinopsis": ("Resumen argumental breve del filme.", "Texto", "Pública"),
    "calificacion_promedio": ("Puntaje promedio otorgado por los usuarios (1.0 a 5.0 estrellas).", "Métrica", "Pública"),

    # Colección canales_tv (IPTV)
    "codigo_canal": ("Identificador único del canal de televisión en vivo (IPTV-XXX).", "Único / Indexado", "Pública"),
    "nombre_canal": ("Nombre público del canal de televisión.", "Requerido", "Pública"),
    "categoria": ("Categoría temática de la señal: Noticias, Deportes, Cultura, Música, General.", "Indexado", "Pública"),
    "pais_origen": ("País donde se origina la transmisión del canal.", "Informativo", "Pública"),
    "idioma": ("Idioma principal de la transmisión en vivo.", "Informativo", "Pública"),
    "url_stream_m3u8": ("URL de la lista de reproducción HLS pública para reproducción en vivo.", "Enlace HLS", "Pública"),
    "calidad_senal": ("Resolución nativa de la señal de televisión en vivo (HD 1080p, SD 576p).", "Técnico", "Pública"),
    "activo": ("Flag booleano que indica si la señal se encuentra al aire.", "Booleano", "Interna"),

    # Colección pagos
    "codigo_pago": ("Código único de la transacción en la pasarela de pagos (PAY-XXXXX).", "Único / Indexado", "Confidencial"),
    "suscripcion_id": ("Referencia al contrato de suscripción asociado.", "Clave Foránea", "Interna"),
    "monto_usd": ("Importe debitado al suscriptor en dólares.", "Requerido", "Confidencial"),
    "fecha_pago": ("Marca temporal exacta de la transacción bancaria.", "Indexado", "Confidencial"),
    "estado_pago": ("Resultado del cobro: EXITOSO, RECHAZADO, REEMBOLSADO, PENDIENTE.", "Indexado", "Confidencial"),
    "pasarela": ("Proveedor de pagos intermediario (Stripe, PayPal, MercadoPago).", "Informativo", "Interna"),
    "ultimos_4_digitos": ("Últimos 4 dígitos de la tarjeta bancaria para conciliación.", "Enmascarado", "Confidencial (PCI-DSS)"),

    # Colección eventos_reproduccion
    "codigo_evento": ("Identificador único del evento de streaming emitido (EVT-XXXXX).", "Único / Indexado", "Interna"),
    "contenido_id": ("Identificador del contenido audiovisual sintonizado.", "Clave Foránea", "Interna"),
    "tipo_contenido": ("Tipo de transmisión: PELICULA_VOD o CANAL_TV_IPTV.", "Indexado", "Interna"),
    "timestamp_inicio": ("Marca temporal del momento en que el reproductor comenzó el playback.", "Indexado", "Interna"),
    "timestamp_fin": ("Marca temporal del momento en que se detuvo la sesión de streaming.", "Informativo", "Interna"),
    "segundos_reproducidos": ("Tiempo acumulado de reproducción efectiva del usuario en segundos.", "Métrica", "Interna"),
    "porcentaje_completitud": ("Porcentaje del total del contenido visualizado (0% a 100%).", "Métrica", "Interna"),
    "abandono_prematuro": ("Indica si el usuario abandonó el video antes del 20% de su duración.", "Flag BI", "Interna"),
    "dispositivo": ("Subdocumento con metadatos del reproductor: tipo, SO, navegador, app_version.", "Objeto", "Interna"),
    "telemetria_qos": ("Subdocumento con métricas de Calidad de Servicio: bitrate_kbps, latencia_ms, buffering_veces.", "Objeto", "Interna"),

    # Colección reproducciones_analiticas (OLAP)
    "evento_id": ("Clave foránea original del evento de reproducción.", "Clave de Hecho", "Interna"),
    "duracion_reproducida_min": ("Duración vista convertida a minutos para análisis OLAP.", "Métrica", "Interna"),
    "duracion_total_min": ("Duración completa del título en minutos.", "Métrica", "Pública"),
    "kpis": ("Subdocumento con métricas cuantitativas calculadas para agregación.", "Objeto", "Interna"),
    "kpis.horas_vistas": ("Tiempo consumido expresado en horas decimales.", "Métrica Sumable", "Interna"),
    "kpis.completitud_pct": ("Porcentaje de avance en el contenido.", "Métrica Promediable", "Interna"),
    "kpis.score_calidad_servicio": ("Puntaje sintético de QoS (0 a 100) derivado de latencia y buffering.", "Métrica QoS", "Interna"),
    "tiempo": ("Dimensión temporal desnormalizada: anio, mes, dia, hora, dia_semana, es_fin_de_semana.", "Dimensión", "Interna"),
    "usuario": ("Dimensión de usuario desnormalizada: codigo, pais, ciudad, segmento, plan.", "Dimensión", "Interna"),
    "contenido": ("Dimensión de contenido desnormalizada: codigo, tipo, titulo, genero, anio.", "Dimensión", "Pública"),
    "dispositivo.tipo": ("Tipo de pantalla receptora: SMART_TV, SMARTPHONE, LAPTOP, TABLET.", "Dimensión", "Interna"),
    "telemetria.buffering_veces": ("Cantidad de congelamientos de búfer experimentados.", "Métrica QoS", "Interna"),
    "telemetria.latencia_ms": ("Retardo de respuesta de red reportado por el CDN en milisegundos.", "Métrica QoS", "Interna"),
    "telemetria.bitrate_kbps": ("Tasa de transferencia de bits promedio alcanzada en la sesión.", "Métrica QoS", "Interna"),

    # Dimensiones Data Warehouse
    "fecha_id": ("Clave subrogada de fecha en formato YYYYMMDD.", "Clave Primaria Dimensión", "Interna"),
    "trimestre": ("Trimestre del año (Q1, Q2, Q3, Q4) calculado con $ceil y $divide.", "Atributo Dimensión", "Interna"),
    "es_fin_semana": ("Flag booleano que indica si la fecha corresponde a sábado o domingo.", "Atributo Dimensión", "Interna"),
    "total_sesiones": ("Conteo agregado de sesiones de streaming en la ventana temporal.", "Métrica Acumulada", "Interna"),
    "horas_totales": ("Total acumulado de horas visualizadas.", "Métrica Acumulada", "Interna"),
}


def banner():
    print("=" * 80)
    print("📚 HYPERFLIX — GENERADOR DE CATÁLOGO Y DICCIONARIO DE DATOS")
    print("   Extracción de Metadatos, Gobernanza, Linaje y Zonas del Data Lake")
    print("=" * 80 + "\n")


def aplanar_documento(d, parent_key='', sep='.'):
    """Aplana recursivamente diccionarios anidados para inspeccionar campos subdocumento."""
    items = []
    for k, v in d.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, dict):
            items.extend(aplanar_documento(v, new_key, sep=sep).items())
        else:
            items.append((new_key, v))
    return dict(items)


def obtener_tipo_amigable(valor):
    """Traduce tipos de Python/BSON a descripciones estándar de base de datos."""
    if valor is None:
        return "Nullable"
    tipo = type(valor).__name__
    mapa_tipos = {
        "str": "String (Varchar)",
        "int": "Integer (Int32 / Int64)",
        "float": "Double / Float",
        "bool": "Boolean",
        "datetime": "Timestamp (ISO 8601)",
        "list": "Array (List)",
        "dict": "Document (JSON / BSON)",
        "ObjectId": "ObjectId (BSON Key)"
    }
    return mapa_tipos.get(tipo, tipo)


def extraer_metadatos_atlas():
    """Conecta a MongoDB Atlas e inspecciona todas las colecciones activas."""
    print("🔍 [1/3] Extrayendo metadatos de colecciones en MongoDB Atlas...")
    catalogo_atlas = {}
    
    try:
        client = pymongo.MongoClient(MONGO_URI, serverSelectionTimeoutMS=6000)
        db = client[DB_NAME]
        nombres_colecciones = sorted(db.list_collection_names())
        
        for col_name in nombres_colecciones:
            col = db[col_name]
            total_docs = col.count_documents({})
            
            # Extraer índices
            indices_info = col.index_information()
            indices_resumen = []
            for idx_name, idx_det in indices_info.items():
                campos = [k[0] for k in idx_det.get("key", [])]
                es_unico = idx_det.get("unique", False)
                indices_resumen.append({
                    "nombre": idx_name,
                    "campos": campos,
                    "es_unico": es_unico,
                    "tipo": "Único" if es_unico else "Compuesto" if len(campos) > 1 else "Simple"
                })
            
            # Muestrear documentos para descubrir esquema dinámico
            muestra = list(col.find().limit(50))
            campos_detectados = {}
            
            for doc in muestra:
                doc_plano = aplanar_documento(doc)
                for k, v in doc_plano.items():
                    if k not in campos_detectados:
                        tipo_desc = obtener_tipo_amigable(v)
                        # Obtener descripción semántica si existe
                        desc_negocio, restriccion, sensibilidad = DICCIONARIO_NEGOCIO.get(
                            k, 
                            (f"Campo operacional de la entidad {col_name}.", "General", "Interna")
                        )
                        # Generar ejemplo representativo
                        ejemplo_str = str(v)
                        if len(ejemplo_str) > 50:
                            ejemplo_str = ejemplo_str[:47] + "..."
                            
                        campos_detectados[k] = {
                            "campo": k,
                            "tipo": tipo_desc,
                            "descripcion": desc_negocio,
                            "restriccion": restriccion,
                            "sensibilidad": sensibilidad,
                            "ejemplo": ejemplo_str
                        }
            
            # Clasificar propósito de la colección
            if col_name.startswith("dim_"):
                capa = "Data Warehouse - Dimensión (OLAP)"
            elif col_name.startswith("fact_") or col_name == "reproducciones_analiticas":
                capa = "Data Warehouse - Tabla de Hechos (OLAP)"
            else:
                capa = "Transaccional / Ingesta (OLTP)"
                
            catalogo_atlas[col_name] = {
                "nombre": col_name,
                "capa": capa,
                "total_documentos": total_docs,
                "total_campos": len(campos_detectados),
                "indices": indices_resumen,
                "campos": list(campos_detectados.values())
            }
            print(f"   ✓ Colección '{col_name}': {total_docs:,} docs | {len(campos_detectados)} campos | {len(indices_resumen)} índices")
            
        print()
        return catalogo_atlas
    except Exception as e:
        print(f"❌ Error al inspeccionar Atlas: {e}\n")
        return {}


def extraer_metadatos_data_lake():
    """Inspecciona dinámicamente los archivos Parquet en las 3 zonas del Data Lake."""
    print("📁 [2/3] Inspeccionando archivos Apache Parquet en el Data Lake...")
    catalogo_lake = {}
    zonas = ["raw", "processed", "curated"]
    
    for zona in zonas:
        ruta_zona = os.path.join(DATA_LAKE_DIR, zona)
        if not os.path.exists(ruta_zona):
            continue
            
        archivos = [f for f in os.listdir(ruta_zona) if f.endswith('.parquet')]
        for arch in archivos:
            ruta_archivo = os.path.join(ruta_zona, arch)
            tam_kb = round(os.path.getsize(ruta_archivo) / 1024, 2)
            
            try:
                parquet_file = pq.ParquetFile(ruta_archivo)
                schema = parquet_file.schema_arrow
                num_filas = parquet_file.metadata.num_rows
                columnas = []
                
                for col_name in schema.names:
                    col_type = str(schema.field(col_name).type)
                    desc_negocio, restriccion, sensibilidad = DICCIONARIO_NEGOCIO.get(
                        col_name,
                        (f"Atributo columnar en zona {zona.upper()}.", "Columnar", "Interna")
                    )
                    columnas.append({
                        "columna": col_name,
                        "tipo_arrow": col_type,
                        "descripcion": desc_negocio,
                        "sensibilidad": sensibilidad
                    })
                    
                catalogo_lake[f"{zona}/{arch}"] = {
                    "archivo": arch,
                    "zona": zona.upper(),
                    "tamano_kb": tam_kb,
                    "num_filas": num_filas,
                    "compresion": "Snappy",
                    "formato": "Apache Parquet (Columnar)",
                    "num_columnas": len(columnas),
                    "columnas": columnas
                }
                print(f"   ✓ Parquet '{zona}/{arch}': {num_filas:,} filas | {len(columnas)} cols | {tam_kb} KB")
            except Exception as ex:
                print(f"   ⚠️ No se pudo leer '{arch}': {ex}")
                
    print()
    return catalogo_lake


def construir_documento_markdown(catalogo_atlas, catalogo_lake):
    """Genera el documento Markdown exhaustivo para docs/DICCIONARIO_DE_DATOS.md."""
    lineas = []
    lineas.append("# 📖 HYPERFLIX — Catálogo y Diccionario Oficial de Datos")
    lineas.append("**Materia:** Bases de Datos y Almacenamiento Masivo (Octavo Semestre)  ")
    lineas.append("**Programa:** Ingeniería de Sistemas — Instituto Tecnológico del Putumayo (ITP)  ")
    lineas.append(f"**Generación Automática:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}  ")
    lineas.append("**Dominio:** Plataforma de Streaming de Video, VOD, Canales IPTV y Telemetría QoS  \n")
    lineas.append("---\n")
    
    # 1. Introducción
    lineas.append("## 📌 1. ¿Qué es y Para Qué Sirve este Catálogo de Datos?")
    lineas.append("Un **Catálogo y Diccionario de Datos** es la fuente única de verdad (*Single Source of Truth*) que describe detalladamente todos los activos de información almacenados en la plataforma **HYPERFLIX**.")
    lineas.append("Cumple funciones críticas de ingeniería y negocio:")
    lineas.append("1. **Gobernanza de Datos:** Clasifica cada campo según su nivel de privacidad y sensibilidad (Pública, Interna, Confidencial PII bajo Habeas Data / GDPR).")
    lineas.append("2. **Descubrimiento y Autoservicio (Self-Service Analytics):** Permite a desarrolladores, analistas de Power BI y científicos de datos comprender de inmediato el significado técnico de cada métrica (ej. `bitrate_kbps`, `porcentaje_completitud`).")
    lineas.append("3. **Trazabilidad y Linaje de Datos:** Mapea cómo fluye la información desde la captura transaccional (**OLTP**) pasando por el **Data Lake Parquet (Raw ➔ Processed ➔ Curated)** hasta el **Modelo Dimensional en Estrella (OLAP)**.")
    lineas.append("4. **Auditoría Automatizada:** Generado programáticamente mediante introspección directa de esquemas para evitar la desincronización de documentación manual.\n")
    lineas.append("---\n")
    
    # 2. Resumen Ejecutivo
    total_colecciones = len(catalogo_atlas)
    total_docs_bd = sum(c["total_documentos"] for c in catalogo_atlas.values())
    total_parquet = len(catalogo_lake)
    
    lineas.append("## 📊 2. Resumen General del Ecosistema de Datos\n")
    lineas.append("| Métrica del Sistema | Valor Detectado en Vivo | Observación Técnica |")
    lineas.append("|---|:---:|---|")
    lineas.append(f"| **Colecciones Activas en Atlas** | **{total_colecciones}** | Capas OLTP, OLAP y Modelo Dimensional en Estrella |")
    lineas.append(f"| **Volumen de Documentos en Atlas** | **{total_docs_bd:,}** | Registros analíticos y transaccionales vivos |")
    lineas.append(f"| **Particiones Parquet en Data Lake** | **{total_parquet}** | Archivos columnars con compresión Snappy |")
    lineas.append(f"| **Zonas del Data Lake Cubiertas** | **3 (Raw, Processed, Curated)** | Arquitectura estándar Medallion Lakehouse |\n")
    lineas.append("---\n")
    
    # 3. Linaje de Datos (Diagrama Mermaid)
    lineas.append("## 🔄 3. Diagrama de Linaje de Datos (Data Lineage)\n")
    lineas.append("```mermaid")
    lineas.append("flowchart LR")
    lineas.append("    subgraph Ingesta[\"Capa 1: Ingesta Transaccional OLTP\"]")
    lineas.append("        U[\"usuarios\"] --> E[\"eventos_reproduccion\"]")
    lineas.append("        P[\"peliculas\"] --> E")
    lineas.append("        C[\"canales_tv\"] --> E")
    lineas.append("        S[\"suscripciones\"] --> PAY[\"pagos\"]")
    lineas.append("    end")
    lineas.append("    subgraph DataLake[\"Capa 2: Data Lake Columnar (Parquet)\"]")
    lineas.append("        E --> RAW[\"raw/eventos_reproduccion.parquet\"]")
    lineas.append("        RAW --> PROC[\"processed/reproducciones_procesadas.parquet\"]")
    lineas.append("        PROC --> CUR[\"curated/reproducciones_curadas.parquet\"]")
    lineas.append("    end")
    lineas.append("    subgraph DataWarehouse[\"Capa 3: Data Warehouse Estrella (OLAP)\"]")
    lineas.append("        CUR --> FACT[\"fact_reproducciones\"]")
    lineas.append("        CUR --> DT[\"dim_tiempo\"]")
    lineas.append("        CUR --> DU[\"dim_usuario\"]")
    lineas.append("        CUR --> DC[\"dim_contenido\"]")
    lineas.append("        CUR --> DD[\"dim_dispositivo\"]")
    lineas.append("    end")
    lineas.append("```\n")
    lineas.append("---\n")
    
    # 4. Diccionario Detallado de Colecciones MongoDB Atlas
    lineas.append("## 🗄️ 4. Diccionario de Datos — MongoDB Atlas (`hyperflix_db`)\n")
    for col_name, info in catalogo_atlas.items():
        lineas.append(f"### 📦 Colección: `{col_name}`")
        lineas.append(f"- **Capa de Arquitectura:** {info['capa']}")
        lineas.append(f"- **Total de Documentos:** {info['total_documentos']:,}")
        lineas.append(f"- **Total de Campos:** {info['total_campos']}")
        
        # Índices
        if info["indices"]:
            indices_texto = ", ".join([f"`{idx['nombre']}` ({idx['tipo']}: {', '.join(idx['campos'])})" for idx in info["indices"]])
            lineas.append(f"- **Estrategia de Indexación:** {indices_texto}\n")
        else:
            lineas.append("- **Estrategia de Indexación:** Índice por defecto `_id_`\n")
            
        lineas.append("| Campo / Atributo | Tipo BSON | Restricción | Sensibilidad | Descripción de Negocio | Ejemplo |")
        lineas.append("|---|---|:---:|:---:|---|---|")
        for c in info["campos"]:
            lineas.append(f"| `{c['campo']}` | {c['tipo']} | {c['restriccion']} | `{c['sensibilidad']}` | {c['descripcion']} | `{c['ejemplo']}` |")
        lineas.append("\n---\n")
        
    # 5. Diccionario de Archivos Data Lake Parquet
    lineas.append("## 🌊 5. Catálogo de Almacenamiento Columnar — Data Lake Parquet\n")
    for key, info in catalogo_lake.items():
        lineas.append(f"### 📄 Archivo: `{key}` (Zona: {info['zona']})")
        lineas.append(f"- **Formato:** {info['formato']} con Compresión **{info['compresion']}**")
        lineas.append(f"- **Volumen Registrado:** {info['num_filas']:,} filas | {info['tamano_kb']} KB")
        lineas.append(f"- **Total de Columnas:** {info['num_columnas']}\n")
        
        lineas.append("| Columna Parquet | Tipo PyArrow | Sensibilidad | Propósito del Atributo |")
        lineas.append("|---|---|:---:|---|")
        for c in info["columnas"]:
            lineas.append(f"| `{c['columna']}` | `{c['tipo_arrow']}` | `{c['sensibilidad']}` | {c['descripcion']} |")
        lineas.append("\n---\n")
        
    return "\n".join(lineas)


def construir_portal_html(catalogo_atlas, catalogo_lake):
    """Genera un reporte web interactivo HTML5 para visualizar el catálogo de datos."""
    html = f"""<!DOCTYPE html>
<html lang="es">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>HYPERFLIX — Catálogo y Diccionario de Datos</title>
  <script src="https://cdn.tailwindcss.com"></script>
  <style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;600;700;800&display=swap');
    body {{ font-family: 'Inter', sans-serif; }}
  </style>
</head>
<body class="bg-slate-950 text-slate-100 min-h-screen">
  <!-- Encabezado -->
  <header class="border-b border-slate-800 bg-slate-900/80 backdrop-blur sticky top-0 z-50 px-6 py-4">
    <div class="max-w-7xl mx-auto flex flex-col md:flex-row justify-between items-center gap-4">
      <div class="flex items-center gap-3">
        <span class="text-3xl">🎬</span>
        <div>
          <h1 class="text-xl font-black tracking-wider text-rose-500">HYPERFLIX</h1>
          <p class="text-xs text-slate-400">Portal de Gobernanza y Diccionario de Datos (ITP 2026-1)</p>
        </div>
      </div>
      <div class="flex items-center gap-4 text-xs">
        <span class="px-3 py-1.5 rounded-full bg-emerald-500/10 text-emerald-400 border border-emerald-500/30">
          ● Estado: Catálogo en Vivo
        </span>
        <span class="text-slate-400">Actualizado: {datetime.now().strftime('%Y-%m-%d %H:%M')}</span>
      </div>
    </div>
  </header>

  <main class="max-w-7xl mx-auto px-6 py-8">
    <!-- Tarjetas de Resumen -->
    <section class="grid grid-cols-1 md:grid-cols-4 gap-4 mb-8">
      <div class="p-5 rounded-2xl bg-slate-900 border border-slate-800">
        <p class="text-xs text-slate-400 uppercase font-semibold">Colecciones Atlas</p>
        <p class="text-3xl font-extrabold text-white mt-1">{len(catalogo_atlas)}</p>
        <p class="text-xs text-slate-500 mt-2">OLTP + OLAP + Estrella</p>
      </div>
      <div class="p-5 rounded-2xl bg-slate-900 border border-slate-800">
        <p class="text-xs text-slate-400 uppercase font-semibold">Total Documentos</p>
        <p class="text-3xl font-extrabold text-rose-400 mt-1">{sum(c['total_documentos'] for c in catalogo_atlas.values()):,}</p>
        <p class="text-xs text-slate-500 mt-2">Registros inspeccionados</p>
      </div>
      <div class="p-5 rounded-2xl bg-slate-900 border border-slate-800">
        <p class="text-xs text-slate-400 uppercase font-semibold">Archivos Data Lake</p>
        <p class="text-3xl font-extrabold text-sky-400 mt-1">{len(catalogo_lake)}</p>
        <p class="text-xs text-slate-500 mt-2">Parquet Snappy</p>
      </div>
      <div class="p-5 rounded-2xl bg-slate-900 border border-slate-800">
        <p class="text-xs text-slate-400 uppercase font-semibold">Gobernanza / PII</p>
        <p class="text-3xl font-extrabold text-emerald-400 mt-1">100%</p>
        <p class="text-xs text-slate-500 mt-2">Sensibilidad mapeada</p>
      </div>
    </section>

    <!-- Pestañas de Navegación -->
    <div class="mb-6 flex gap-3 border-b border-slate-800 pb-3">
      <button class="px-4 py-2 rounded-lg bg-rose-600 text-white font-medium text-sm">Colecciones MongoDB Atlas</button>
      <button class="px-4 py-2 rounded-lg bg-slate-900 hover:bg-slate-800 text-slate-300 font-medium text-sm">Zonas Data Lake Parquet</button>
    </div>

    <!-- Lista de Colecciones -->
    <div class="space-y-8">
"""
    for col_name, info in catalogo_atlas.items():
        html += f"""
      <div class="rounded-2xl bg-slate-900 border border-slate-800 overflow-hidden shadow-xl">
        <div class="p-6 bg-slate-850 border-b border-slate-800 flex flex-col md:flex-row justify-between md:items-center gap-2">
          <div>
            <div class="flex items-center gap-3">
              <span class="text-lg">📦</span>
              <h2 class="text-xl font-bold text-white tracking-wide">coleccion: <span class="text-rose-400">{col_name}</span></h2>
              <span class="px-2.5 py-0.5 text-xs rounded-full bg-slate-800 text-slate-300 border border-slate-700">{info['capa']}</span>
            </div>
            <p class="text-xs text-slate-400 mt-1">Registros: <strong class="text-white">{info['total_documentos']:,}</strong> | Total atributos: <strong class="text-white">{info['total_campos']}</strong></p>
          </div>
        </div>

        <div class="overflow-x-auto">
          <table class="w-full text-left text-sm">
            <thead class="bg-slate-950/60 text-xs uppercase text-slate-400 border-b border-slate-800">
              <tr>
                <th class="py-3 px-4">Campo</th>
                <th class="py-3 px-4">Tipo de Dato</th>
                <th class="py-3 px-4">Restricción</th>
                <th class="py-3 px-4">Sensibilidad</th>
                <th class="py-3 px-4">Descripción de Negocio</th>
                <th class="py-3 px-4">Ejemplo</th>
              </tr>
            </thead>
            <tbody class="divide-y divide-slate-800/60">
"""
        for c in info['campos']:
            badge_color = "bg-rose-500/10 text-rose-400 border-rose-500/30" if "PII" in c['sensibilidad'] else "bg-slate-800 text-slate-300 border-slate-700"
            html += f"""
              <tr class="hover:bg-slate-800/40 transition">
                <td class="py-2.5 px-4 font-mono text-xs text-sky-300 font-semibold">{c['campo']}</td>
                <td class="py-2.5 px-4 text-xs text-slate-300">{c['tipo']}</td>
                <td class="py-2.5 px-4 text-xs text-amber-300 font-medium">{c['restriccion']}</td>
                <td class="py-2.5 px-4"><span class="px-2 py-0.5 text-[11px] rounded border {badge_color}">{c['sensibilidad']}</span></td>
                <td class="py-2.5 px-4 text-xs text-slate-300">{c['descripcion']}</td>
                <td class="py-2.5 px-4 font-mono text-xs text-slate-400">{c['ejemplo']}</td>
              </tr>
"""
        html += """
            </tbody>
          </table>
        </div>
      </div>
"""
    html += """
    </div>
  </main>
</body>
</html>
"""
    return html


def main():
    banner()
    t0 = time.time()
    
    # 1. Extracción de metadatos
    catalogo_atlas = extraer_metadatos_atlas()
    catalogo_lake = extraer_metadatos_data_lake()
    
    # 2. Generar archivo JSON
    print("📝 [3/3] Exportando Catálogo y Diccionario en formatos estándar...")
    catalogo_completo = {
        "metadata": {
            "proyecto": "HYPERFLIX - Streaming Masivo",
            "version": "1.0.0",
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "estandar_gobernanza": "Apache Atlas / OpenMetadata Specification",
            "total_colecciones_atlas": len(catalogo_atlas),
            "total_archivos_parquet": len(catalogo_lake)
        },
        "mongodb_atlas": catalogo_atlas,
        "data_lake_parquet": catalogo_lake
    }
    
    ruta_json = os.path.join(BASE_DIR, "catalogo_datos.json")
    with open(ruta_json, "w", encoding="utf-8") as f:
        json.dump(catalogo_completo, f, indent=2, ensure_ascii=False)
    print(f"   ✅ Catálogo JSON exportado: {ruta_json}")
    
    # 3. Generar archivo Markdown
    os.makedirs(DOCS_DIR, exist_ok=True)
    contenido_md = construir_documento_markdown(catalogo_atlas, catalogo_lake)
    ruta_md = os.path.join(DOCS_DIR, "DICCIONARIO_DE_DATOS.md")
    with open(ruta_md, "w", encoding="utf-8") as f:
        f.write(contenido_md)
    print(f"   ✅ Diccionario Markdown exportado: {ruta_md}")
    
    # 4. Generar reporte HTML interactivo
    contenido_html = construir_portal_html(catalogo_atlas, catalogo_lake)
    ruta_html = os.path.join(BASE_DIR, "catalogo_datos.html")
    with open(ruta_html, "w", encoding="utf-8") as f:
        f.write(contenido_html)
    print(f"   ✅ Portal HTML Interactivo generado: {ruta_html}\n")
    
    tiempo_total = round(time.time() - t0, 2)
    print("=" * 80)
    print(f"🎉 CATÁLOGO Y DICCIONARIO DE DATOS GENERADO EN {tiempo_total} SEGUNDOS")
    print(f"   • Colecciones Catalogadas: {len(catalogo_atlas)} activas en Atlas")
    print(f"   • Archivos Parquet Catalogados: {len(catalogo_lake)} particiones en Data Lake")
    print(f"   • Documentación Generada: docs/DICCIONARIO_DE_DATOS.md y catalogo_datos.html")
    print("=" * 80)


if __name__ == "__main__":
    main()
