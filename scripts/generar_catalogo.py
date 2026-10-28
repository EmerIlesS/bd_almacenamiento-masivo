"""
=============================================================================
GENERADOR PROFESIONAL DE CATÁLOGO Y DICCIONARIO DE DATOS EN EXCEL
=============================================================================
Materia:   Bases de Datos y Almacenamiento Masivo
Propósito: Inspeccionar la base de datos real en MongoDB Atlas,
           aplanar documentos JSON anidados, mapear índices y generar
           el archivo Excel oficial con dos pestañas técnicas:
             1. Resumen Arquitectónico (OLTP, OLAP, Data Warehouse)
             2. Diccionario de Datos (Campos, Tipos, PK/FK, Índices, Negocio)
=============================================================================
Uso:
    python scripts/generar_catalogo.py
=============================================================================
"""

import os
import sys
from datetime import datetime
import pymongo
import pandas as pd
from dotenv import load_dotenv

# Codificación UTF-8 para consola de Windows
if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8')

# Cargar variables de entorno
load_dotenv()
MONGO_URI = os.getenv("MONGO_URI")

# Determinar base de datos objetivo:
# Prioriza HYPERFLIX_DB si existe, o ventas_db según configuración
TARGET_DB = os.getenv("MONGO_DB_NAME", "hyperflix_db")

# Diccionario enriquecido de descripciones de negocio para el catálogo
DICCIONARIO_NEGOCIO = {
    # Claves universales
    "_id": ("Identificador único e inmutable autogenerado por MongoDB (BSON ObjectId).", "PK", "Técnico"),
    
    # HYPERFLIX - usuarios / suscriptores
    "codigo_usuario": ("Código único de identificación de cliente (formato USR-XXXXX).", "Unique", "Interna"),
    "nombre": ("Nombre completo o razón social del suscriptor titular.", "Requerido", "Confidencial (PII)"),
    "correo": ("Correo electrónico corporativo o personal para acceso y facturación.", "Unique / Index", "Confidencial (PII)"),
    "email": ("Dirección de correo electrónico para notificaciones y login.", "Unique / Index", "Confidencial (PII)"),
    "pais": ("País de origen o residencia para segmentación de catálogo y licencias.", "Index", "Interna"),
    "ciudad": ("Ciudad geográfica para análisis de penetración de mercado.", "General", "Interna"),
    "telefono": ("Número telefónico de contacto para seguridad y doble factor.", "Opcional", "Confidencial (PII)"),
    "direccion": ("Estructura anidada con datos de domicilio físico del cliente.", "Subdocumento", "Confidencial (PII)"),
    "direccion.calle": ("Vía pública y nomenclatura del domicilio registrado.", "General", "Confidencial (PII)"),
    "direccion.ciudad": ("Ciudad del domicilio reportado por el usuario.", "Index", "Interna"),
    "direccion.pais": ("País del domicilio reportado para cumplimiento fiscal.", "General", "Interna"),
    "segmento": ("Clasificación comercial del suscriptor (VIP, ESTUDIANTE, FREEMIUM, REGULAR).", "Index", "Interna"),
    "dispositivo_preferido": ("Dispositivo principal de consumo multimedia (Smart TV, Smartphone, PC).", "General", "Interna"),
    "fecha_registro": ("Marca temporal del registro inicial en la plataforma.", "Audit", "Interna"),
    "activo": ("Flag booleano que indica si el usuario está habilitado en el sistema.", "Booleano", "Interna"),
    
    # HYPERFLIX - suscripciones
    "codigo_suscripcion": ("Código del contrato de membresía (formato SUB-XXXXX).", "Unique", "Interna"),
    "usuario_id": ("Identificador foráneo del usuario titular de la cuenta.", "FK", "Interna"),
    "cliente_id": ("Referencia al cliente titular de la transacción comercial.", "FK", "Interna"),
    "plan": ("Modalidad de servicio contratada (BASICO 720p, ESTANDAR 1080p, PREMIUM 4K).", "Index", "Pública"),
    "precio_usd": ("Tarifa mensual facturada en dólares estadounidenses.", "Monto", "Interna"),
    "fecha_inicio": ("Fecha de inicio de vigencia del período de suscripción.", "Audit", "Interna"),
    "fecha_renovacion": ("Fecha proyectada para el siguiente débito recurrente.", "Audit", "Interna"),
    "fecha_fin": ("Fecha límite o de expiración del plan actual.", "Audit", "Interna"),
    "estado": ("Estado de la cuenta: ACTIVA, CANCELADA, EN_MORA, PAUSADA.", "Index", "Interna"),
    "metodo_pago_preferido": ("Forma de pago registrada: TARJETA, PAYPAL, PSE, CRIPTO.", "General", "Confidencial"),
    "metodo_pago": ("Medio de pago procesado en la transacción de cobro.", "General", "Confidencial"),

    # HYPERFLIX - peliculas / catálogo VOD
    "codigo_pelicula": ("Código de inventario audiovisual (formato MOV-XXXXX).", "Unique / Index", "Pública"),
    "sku": ("Código SKU de inventario único para productos y contenidos.", "Unique / Index", "Interna"),
    "titulo": ("Título oficial comercial de la obra cinematográfica o serie.", "Index", "Pública"),
    "nombre_producto": ("Denominación del ítem en catálogo.", "General", "Pública"),
    "genero": ("Categoría temática principal (Acción, Ciencia Ficción, Drama, Comedia).", "Index", "Pública"),
    "categoria": ("Línea de producto o clasificación temática de catálogo.", "Index", "Pública"),
    "generos_secundarios": ("Lista de subgéneros asociados a la obra audiovisual.", "Array", "Pública"),
    "anio_lanzamiento": ("Año de estreno comercial a nivel global.", "Filtro", "Pública"),
    "duracion_minutos": ("Duración total del título audiovisual expresada en minutos.", "Métrica", "Pública"),
    "duracion_segundos": ("Duración exacta en segundos para control de reproducción y bitrate.", "Técnico", "Interna"),
    "clasificacion_edad": ("Clasificación de edad orientativa (APTA, +13, +16, +18).", "Parental", "Pública"),
    "resolucion_maxima": ("Calidad de video disponible (4K UHD, 1080p FHD, 720p HD).", "Técnico", "Pública"),
    "calificacion_promedio": ("Calificación promedio asignada por la audiencia (1.0 a 5.0).", "Métrica BI", "Pública"),
    "precio_unitario": ("Valor monetario unitario asignado al producto o licencia.", "Financiero", "Pública"),
    "stock": ("Unidades disponibles de inventario o cupos para consumo simultáneo.", "Operativo", "Interna"),
    "proveedor": ("Distribuidor, estudio cinematográfico o casa productora asociada.", "General", "Interna"),
    "fecha_creacion": ("Marca temporal de alta del contenido en el catálogo maestro.", "Audit", "Interna"),

    # HYPERFLIX - canales de TV en vivo (IPTV)
    "codigo_canal": ("Identificador unívoco de señal de televisión pública (IPTV-XXX).", "Unique / Index", "Pública"),
    "nombre_canal": ("Denominación pública de la señal de televisión en vivo.", "Index", "Pública"),
    "pais_origen": ("País de procedencia de la señal de transmisión en directo.", "Filtro", "Pública"),
    "idioma": ("Idioma primario de la emisión audiovisual.", "General", "Pública"),
    "url_stream_m3u8": ("Enlace directo HLS (m3u8) para reproducción libre en tiempo real.", "Stream URL", "Pública"),
    "calidad_senal": ("Resolución nativa de transmisión (Full HD 1080p, HD 720p).", "Técnico", "Pública"),

    # HYPERFLIX - pagos y transacciones
    "codigo_pago": ("Código único de autorización de la pasarela bancaria (PAY-XXXXX).", "Unique / Index", "Confidencial"),
    "numero_factura": ("Número consecutivo legal de facturación emitido (FAC-YYYY-XXXX).", "Unique / Index", "Confidencial"),
    "suscripcion_id": ("Referencia al contrato de suscripción vinculado al pago.", "FK", "Interna"),
    "monto_usd": ("Valor total de la operación liquidado en dólares estadounidenses.", "Financiero", "Confidencial"),
    "total": ("Importe bruto final de la factura o transacción comercial.", "Financiero", "Confidencial"),
    "fecha_pago": ("Marca temporal precisa en que el banco confirmó la transacción.", "Index", "Confidencial"),
    "fecha_venta": ("Fecha y hora exacta en que se registró la compra o consumo.", "Index", "Confidencial"),
    "estado_pago": ("Estado de conciliación: EXITOSO, RECHAZADO, PENDIENTE, REVERSADO.", "Index", "Confidencial"),
    "pasarela": ("Operador de pagos tecnológico intermediario (Stripe, PSE, PayPal).", "General", "Interna"),
    "ultimos_4_digitos": ("Últimos 4 dígitos de la tarjeta para auditoría y comprobante.", "Enmascarado", "Confidencial (PCI-DSS)"),

    # HYPERFLIX - eventos_reproduccion y telemetría de streaming
    "codigo_evento": ("Identificador único del evento telemétrico de streaming (EVT-XXXXX).", "Unique / Index", "Interna"),
    "contenido_id": ("Identificador del contenido audiovisual o canal reproducido.", "FK", "Interna"),
    "tipo_contenido": ("Modalidad de transmisión: PELICULA_VOD o CANAL_TV_IPTV.", "Index", "Interna"),
    "timestamp_inicio": ("Marca temporal de inicio de sesión de streaming en el cliente.", "Index", "Interna"),
    "timestamp_fin": ("Marca temporal de finalización o cierre de la sesión de video.", "General", "Interna"),
    "segundos_reproducidos": ("Tiempo neto efectivo de video consumido por el usuario.", "Métrica Streaming", "Interna"),
    "porcentaje_completitud": ("Porcentaje visto respecto a la duración total del video (0 a 100%).", "KPI Algoritmo", "Interna"),
    "abandono_prematuro": ("Flag booleano que señala si se abandonó antes del 20% de duración.", "KPI Churn", "Interna"),
    "dispositivo": ("Estructura anidada con especificaciones técnicas del reproductor.", "Subdocumento", "Interna"),
    "dispositivo.tipo": ("Tipo de pantalla receptora (SMART_TV, SMARTPHONE, LAPTOP, TABLET).", "Dimensión", "Interna"),
    "dispositivo.sistema_operativo": ("Sistema operativo del reproductor (Android, iOS, Windows, Tizen).", "Dimensión", "Interna"),
    "dispositivo.navegador": ("Navegador web o cliente nativo de reproducción.", "Técnico", "Interna"),
    "dispositivo.app_version": ("Versión de la aplicación cliente instalada.", "Audit", "Interna"),
    "telemetria_qos": ("Métricas telemétricas de Calidad de Servicio (QoS).", "Subdocumento", "Interna"),
    "telemetria_qos.bitrate_kbps": ("Tasa media de transferencia de bits alcanzada en kbps.", "Métrica QoS", "Interna"),
    "telemetria_qos.latencia_ms": ("Retardo de red reportado por el CDN en milisegundos.", "Métrica QoS", "Interna"),
    "telemetria_qos.buffering_veces": ("Cantidad de pausas por búfer experimentadas en la sesión.", "Métrica QoS", "Interna"),

    # HYPERFLIX - modelo dimensional analítico (OLAP)
    "evento_id": ("Clave foránea de origen del evento de streaming crudo.", "FK Hecho", "Interna"),
    "duracion_reproducida_min": ("Tiempo de visualización estandarizado en minutos para OLAP.", "Métrica Hecho", "Interna"),
    "duracion_total_min": ("Duración total de la obra audiovisual en minutos.", "Métrica Hecho", "Pública"),
    "kpis": ("Estructura agregada de indicadores de rendimiento del negocio.", "Subdocumento", "Interna"),
    "kpis.horas_vistas": ("Horas decimales totales consumidas en streaming.", "Métrica Aditiva", "Interna"),
    "kpis.completitud_pct": ("Porcentaje de avance sobre la obra audiovisual.", "Métrica Promedio", "Interna"),
    "kpis.score_calidad_servicio": ("Puntaje calculado de Calidad de Experiencia QoE (0 a 100).", "Métrica BI", "Interna"),
    "ganancia_estimada": ("Margen comercial estimado sobre la transacción.", "Monto BI", "Confidencial"),
    "anio": ("Año del calendario para agrupamiento de series temporales.", "Dimensión Tiempo", "Interna"),
    "mes": ("Mes del calendario (1 a 12) para tendencias mensuales.", "Dimensión Tiempo", "Interna"),
    "dia": ("Día del mes (1 a 31) para análisis de comportamiento diario.", "Dimensión Tiempo", "Interna"),
    "dia_semana": ("Día de la semana (1=Domingo a 7=Sábado) para picos de audiencia.", "Dimensión Tiempo", "Interna"),
}


def aplanar_diccionario(doc, parent_key='', sep='.'):
    """Aplana diccionarios y subdocumentos anidados recursivamente."""
    items = []
    for k, v in doc.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, dict):
            items.extend(aplanar_diccionario(v, new_key, sep=sep).items())
        elif isinstance(v, list) and len(v) > 0 and isinstance(v[0], dict):
            # Representar primer elemento de un array de objetos
            items.extend(aplanar_diccionario(v[0], f"{new_key}[]", sep=sep).items())
        else:
            items.append((new_key, v))
    return dict(items)


def normalizar_tipo(valor):
    """Estandariza los tipos de datos dinámicos a convenciones formales."""
    if valor is None:
        return "Nullable"
    nombre_tipo = type(valor).__name__
    mapa = {
        "str": "String",
        "int": "Integer",
        "float": "Float / Double",
        "bool": "Boolean",
        "datetime": "Timestamp",
        "list": "Array",
        "dict": "Document (JSON)",
        "ObjectId": "ObjectId (BSON)"
    }
    return mapa.get(nombre_tipo, nombre_tipo.capitalize())


def clasificar_rol_arquitectura(col_name):
    """Clasifica la colección según su rol en la arquitectura de datos moderna."""
    col_lower = col_name.lower()
    if col_lower.startswith("dim_"):
        return "Data Warehouse (Dimensión OLAP)"
    elif col_lower.startswith("fact_") or col_lower in ["reproducciones_analiticas", "ventas_analiticas"]:
        return "Data Warehouse (Tabla de Hechos OLAP)"
    elif any(k in col_lower for k in ["evento", "telemetria", "log", "streaming"]):
        return "Ingesta Masiva de Eventos (Event Streaming / Telemetry)"
    else:
        return "Operacional Transaccional (OLTP)"


def detectar_clave(campo):
    """Determina si un campo representa una clave primaria (PK) o foránea (FK)."""
    campo_lower = campo.lower()
    if campo == "_id" or campo.endswith("._id"):
        return "Primary Key (PK)"
    elif campo.endswith("_id") or campo.startswith("id_") or "usuario_id" in campo_lower or "cliente_id" in campo_lower or "producto_id" in campo_lower or "contenido_id" in campo_lower or "suscripcion_id" in campo_lower:
        return "Foreign Key (FK)"
    elif campo.startswith("codigo_") or campo in ["numero_factura", "sku"]:
        return "Clave de Negocio (Natural Key)"
    return "Atributo Descriptivo"


def inspeccionar_base_datos(db):
    """Inspecciona dinámicamente las colecciones de la base de datos."""
    print(f"🔍 Conectando a MongoDB Atlas y analizando base de datos '{db.name}'...")
    nombres_colecciones = sorted(db.list_collection_names())
    
    filas_resumen = []
    filas_diccionario = []
    
    for col_name in nombres_colecciones:
        col = db[col_name]
        total_documentos = col.count_documents({})
        
        # Mapear índices
        indices_info = col.index_information()
        campos_indexados = set()
        detalles_indices = []
        
        for idx_name, idx_meta in indices_info.items():
            keys = [k[0] for k in idx_meta.get("key", [])]
            for k in keys:
                campos_indexados.add(k)
            es_unico = " (Único)" if idx_meta.get("unique") else ""
            detalles_indices.append(f"{idx_name}: {', '.join(keys)}{es_unico}")
            
        resumen_indices_str = "; ".join(detalles_indices) if detalles_indices else "Índice default _id_"
        
        # Inspeccionar esquema tomando una muestra de documentos representativos
        muestra = list(col.find().limit(100))
        esquema_detectado = {}
        
        for doc in muestra:
            doc_plano = aplanar_diccionario(doc)
            for campo, valor in doc_plano.items():
                if campo not in esquema_detectado:
                    # Determinar tipo
                    tipo_std = normalizar_tipo(valor)
                    # Determinar si es clave
                    rol_clave = detectar_clave(campo)
                    # Determinar si está indexado
                    campo_raiz = campo.split("[]")[0]
                    esta_indexado = "SÍ (Indexado)" if campo_raiz in campos_indexados or campo in campos_indexados else "NO"
                    
                    # Obtener descripción de negocio profesional
                    desc_negocio, _, _ = DICCIONARIO_NEGOCIO.get(
                        campo_raiz,
                        DICCIONARIO_NEGOCIO.get(
                            campo,
                            (f"Atributo operacional de la entidad '{col_name}'. Permite registrar y consultar el estado de la entidad.", "", "")
                        )
                    )
                    
                    # Ejemplo
                    ejemplo_str = str(valor)
                    if len(ejemplo_str) > 60:
                        ejemplo_str = ejemplo_str[:57] + "..."
                        
                    esquema_detectado[campo] = {
                        "Colección": col_name,
                        "Nombre del Campo": campo,
                        "Tipo de Dato Estandarizado": tipo_std,
                        "Clave (PK / FK)": rol_clave,
                        "Indexado": esta_indexado,
                        "Descripción de Negocio": desc_negocio,
                        "Ejemplo de Valor": ejemplo_str
                    }
                    
        # Agregar a la pestaña 1 (Resumen Arquitectónico)
        rol_arquit = clasificar_rol_arquitectura(col_name)
        filas_resumen.append({
            "Colección": col_name,
            "Rol Arquitectónico": rol_arquit,
            "Total Documentos": total_documentos,
            "Total Atributos": len(esquema_detectado),
            "Total Índices": len(indices_info),
            "Estrategia de Indexación": resumen_indices_str
        })
        
        # Agregar a la pestaña 2 (Diccionario de Datos)
        for campo_info in esquema_detectado.values():
            filas_diccionario.append(campo_info)
            
        print(f"   ✓ Colección '{col_name}': {total_documentos:,} docs | {len(esquema_detectado)} campos | {len(indices_info)} índices")
        
    return filas_resumen, filas_diccionario


def formatear_excel_con_estilo(ruta_archivo):
    """Aplica formato visual profesional, cabeceras en negrita, colores y anchos a las columnas."""
    import openpyxl
    from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
    
    wb = openpyxl.load_workbook(ruta_archivo)
    
    # Colores corporativos (Azul Marino y Slate)
    header_fill = PatternFill(start_color="1E293B", end_color="1E293B", fill_type="solid")
    header_font = Font(name="Calibri", size=11, bold=True, color="FFFFFF")
    data_font = Font(name="Calibri", size=10, color="0F172A")
    alignment_center = Alignment(horizontal="center", vertical="center")
    alignment_left = Alignment(horizontal="left", vertical="center")
    
    thin_border = Border(
        left=Side(style='thin', color='CBD5E1'),
        right=Side(style='thin', color='CBD5E1'),
        top=Side(style='thin', color='CBD5E1'),
        bottom=Side(style='thin', color='CBD5E1')
    )
    
    for sheet_name in wb.sheetnames:
        ws = wb[sheet_name]
        ws.views.sheetView[0].showGridLines = True
        
        # Estilizar encabezados (Fila 1)
        for col_idx in range(1, ws.max_column + 1):
            cell = ws.cell(row=1, column=col_idx)
            cell.fill = header_fill
            cell.font = header_font
            cell.alignment = alignment_center
            cell.border = thin_border
            ws.row_dimensions[1].height = 28
            
        # Estilizar celdas de datos
        for row_idx in range(2, ws.max_row + 1):
            ws.row_dimensions[row_idx].height = 20
            # Cebra de colores suave
            row_fill = PatternFill(start_color="F8FAFC", end_color="F8FAFC", fill_type="solid") if row_idx % 2 == 0 else None
            
            for col_idx in range(1, ws.max_column + 1):
                cell = ws.cell(row=row_idx, column=col_idx)
                cell.font = data_font
                cell.border = thin_border
                if row_fill:
                    cell.fill = row_fill
                # Alinear centradas las columnas cortas
                val_str = str(cell.value or "")
                if val_str in ["SÍ (Indexado)", "NO", "Primary Key (PK)", "Foreign Key (FK)", "Boolean", "Integer"] or val_str.isdigit():
                    cell.alignment = alignment_center
                else:
                    cell.alignment = alignment_left
                    
        # Auto-ajuste inteligente de ancho de columnas
        for col in ws.columns:
            max_len = 0
            col_letter = col[0].column_letter
            for cell in col:
                val = str(cell.value or '')
                max_len = max(max_len, len(val))
            ws.column_dimensions[col_letter].width = min(max(max_len + 3, 12), 65)
            
    wb.save(ruta_archivo)
    wb.close()


def main():
    print("=" * 80)
    print("📚 GENERADOR OFICIAL DE CATÁLOGO Y DICCIONARIO DE DATOS (EXCEL)")
    print("   Cumplimiento de Fase 2: Gobierno de Datos, Discovery y Metadatos NoSQL")
    print("=" * 80 + "\n")
    
    if not MONGO_URI:
        print("❌ Error: MONGO_URI no está definido en el archivo .env.")
        sys.exit(1)
        
    try:
        client = pymongo.MongoClient(MONGO_URI, serverSelectionTimeoutMS=8000)
        client.admin.command('ping')
    except Exception as e:
        print(f"❌ Error de conexión a MongoDB Atlas: {e}")
        sys.exit(1)
        
    # Base de datos oficial y única del proyecto HYPERFLIX
    bases_a_procesar = []
    if len(sys.argv) > 1 and sys.argv[1].strip():
        bases_a_procesar = [sys.argv[1].strip()]
    else:
        bases_a_procesar = ["hyperflix_db"]
        
    archivos_generados = []
    fecha_hoy = datetime.now().strftime("%Y%m%d")
    
    for nombre_db in bases_a_procesar:
        db = client[nombre_db]
        try:
            cols = db.list_collection_names()
            if not cols:
                print(f"⚠️ La base de datos '{nombre_db}' no contiene colecciones. Omitiendo.")
                continue
        except Exception as ex_col:
            print(f"⚠️ No se pudo acceder a la base de datos '{nombre_db}': {ex_col}")
            continue
            
        print(f"\n================================================================================")
        print(f"📦 Procesando catálogo para Base de Datos: '{nombre_db}' ({len(cols)} colecciones)")
        print(f"================================================================================")
        
        # 1. Extraer datos y metadatos
        filas_resumen, filas_diccionario = inspeccionar_base_datos(db)
        
        if not filas_resumen:
            print(f"⚠️ No se extrajeron datos de '{nombre_db}'.")
            continue
            
        # 2. Construir DataFrames de Pandas
        df_resumen = pd.DataFrame(filas_resumen)
        df_diccionario = pd.DataFrame(filas_diccionario)
        
        # 3. Exportar a Excel con las dos pestañas solicitadas
        nombre_limpio_db = "HyperflixDB" if "hyperflix" in nombre_db.lower() else "VentasDB"
        nombre_archivo = f"Catalogo_Datos_{nombre_limpio_db}_{fecha_hoy}.xlsx"
        ruta_salida = os.path.join(os.getcwd(), nombre_archivo)
        
        print(f"\n📊 [Exportando Excel] Escribiendo archivo '{nombre_archivo}' con dos pestañas:")
        print("   1. 'Resumen Arquitectónico' (Inventario y clasificación OLTP / OLAP)")
        print("   2. 'Diccionario de Datos'   (Campos, Tipos, PK/FK, Índices y Negocio)")
        
        with pd.ExcelWriter(ruta_salida, engine="openpyxl") as writer:
            df_resumen.to_excel(writer, sheet_name="Resumen Arquitectónico", index=False)
            df_diccionario.to_excel(writer, sheet_name="Diccionario de Datos", index=False)
            
        # 4. Formatear y embellecer visualmente
        formatear_excel_con_estilo(ruta_salida)
        archivos_generados.append((nombre_archivo, len(df_resumen), len(df_diccionario)))
        print(f"   ✅ Excel generado: {nombre_archivo} ({len(df_resumen)} colecciones, {len(df_diccionario)} atributos)")
        
    print("\n" + "=" * 80)
    print("🎉 RESUMEN DE GENERACIÓN DE CATÁLOGOS:")
    for arch, n_cols, n_attrs in archivos_generados:
        print(f"   • {arch} ➔ {n_cols} colecciones | {n_attrs} atributos catalogados")
    print(f"📍 Archivos guardados en la raíz del proyecto para entrega al docente.")
    print("=" * 80)
    
    client.close()


if __name__ == "__main__":
    main()
