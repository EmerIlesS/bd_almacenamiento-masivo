# 🎬 HYPERFLIX — Arquitectura de Datos, Streaming Masivo & Data Lake

> **Proyecto de Bases de Datos y Almacenamiento Masivo (Octavo Semestre)**  
> **Fase 1 y Fase 2: Análisis de Requisitos, Persistencia Políglota y Data Lake Parquet**  
> **Programa:** Ingeniería de Sistemas — uniputumayo (2026-1)

---

## 📌 Descripción General

**HYPERFLIX** es una plataforma de streaming y análisis de datos a gran escala. Su objetivo es capturar y procesar de forma continua la telemetría y el comportamiento de visualización de **1.000.000 de usuarios** (generando más de **20.000.000 de eventos diarios / 52 GB al día**), alimentando un **Data Lake en formato Apache Parquet con compresión Snappy** y modelos de Machine Learning (Spark MLlib) para recomendaciones personalizadas de baja latencia mediante Redis.

Incluye una **Landing Page interactiva**, catálogo de películas bajo demanda (VOD), reproductor en vivo de **canales de televisión públicos y libres (IPTV)** vía streaming HLS y un **Monitor de Telemetría en Tiempo Real**.

---

## 🛠️ Pila Tecnológica de la Arquitectura

- **Ingesta:** API Gateway (NestJS) + Apache Kafka (tópicos particionados por `user_id`).
- **Persistencia Transaccional (OLTP):** MongoDB Atlas Cloud (Replica Set $\ge 3$ nodos: Usuarios, Suscripciones, Catálogo, Canales IPTV, Pagos).
- **Data Lake (3 Zonas Oficiales):** Formato columnar **Apache Parquet** con compresión **Snappy** (`raw`, `processed`, `curated`).
- **Pipeline ETL:** Python 3.12 + PyMongo + Pandas (`pd.json_normalize`, conversión de `ObjectId`, enriquecimiento temporal).
- **Procesamiento Distribuido:** Apache Spark / PySpark (ETL/ELT y MLlib).
- **Analítica OLAP & BI:** Modelo en Estrella dimensional (`FACT_REPRODUCCIONES`) evaluado con latencias $< 500$ ms.
- **Caché y Recomendaciones:** Redis (Latencia de entrega < 5 ms).
- **Frontend & Streaming:** HTML5, Tailwind CSS, JavaScript ES6, HLS.js.

---

## 📁 Estructura del Repositorio

```text
bd_almacenamiento-masivo/
├── .env.example                      # Plantilla de variables de entorno (MongoDB URI)
├── .gitignore                        # Protección de credenciales .env y entornos .venv
├── conexion.py                       # Módulo de conexión central a MongoDB Atlas con soporte UTF-8
├── data_lake.py                      # Pipeline oficial del Data Lake (Atlas ➔ Pandas ➔ Parquet Snappy)
├── spark_processor.py                # Entrada directa al Motor Big Data de Fase 2 (Apache Spark Job)
├── data_warehouse.py                 # Entrada directa al Data Warehouse / Modelo en Estrella (Fase 3)
├── requirements.txt                  # Dependencias de Python (pymongo, pandas, pyarrow, faker, dotenv, pyspark)
├── README.md                         # Guía general de uso y presentación del proyecto
├── docs/
│   ├── INFORME_PARCIAL_HYPERFLIX.md  # Informe técnico oficial del Parcial Primer Corte (Adaptado a HyperFlix)
│   └── ARQUITECTURA_HYPERFLIX.md     # Documento maestro con especificación formal y diagramas Mermaid
├── scripts/
│   ├── construir_data_warehouse_hyperflix.py  # Construcción del Modelo en Estrella en Atlas (Fase 3 - HyperFlix)
│   ├── construir_data_warehouse_estrella.py   # Construcción del Modelo en Estrella en Atlas (Fase 3 - Guía Docente)
│   ├── spark_job.py                  # Motor Apache Spark Job (explode, withColumn, groupBy+agg, Data Warehouse)
│   ├── crear_estructura.py           # Inicializa colecciones OLTP, índices únicos y datos semilla
│   ├── generar_datos_masivos.py      # Generador de 20.000+ eventos de streaming, 1.500+ usuarios y catálogo
│   ├── transformar_oltp_a_olap.py    # Pipeline ETL de agregación para construir la colección analítica fuente
│   ├── benchmarking_consultas.py     # Benchmarking analítico con medición de latencias (ms) e índices compuestos
│   ├── consultar_parquet.py          # Lector analítico columnar de los archivos Parquet en el Data Lake
│   ├── productor_streaming.py        # Emisor de eventos de telemetría en tiempo real (simulación Kafka)
│   └── consumidor_streaming_cdc.py   # Consumidor Change Streams (simulación Spark Streaming CDC)
├── data_lake/                        # Data Lake estructurado en 3 zonas (Formato Parquet Snappy)
│   ├── raw/                          # usuarios.parquet, peliculas.parquet, canales_tv.parquet, eventos_reproduccion.parquet
│   ├── processed/                    # reproducciones_procesadas.parquet (Datos limpios y enriquecidos con fechas)
│   └── curated/                      # reproducciones_curadas.parquet y kpis_spark_*.parquet (Listo para DW / BI)
└── web/                              # Aplicación Web y Landing Page
    ├── index.html                    # Landing page + Reproductor IPTV HLS + Monitor de Telemetría
    └── app.js                        # Lógica de reproducción, canales libres y telemetría en vivo
```

---

## 🚀 Guía Paso a Paso de Ejecución

### 1. Clonar el Repositorio e Instalar Dependencias
```powershell
git clone git@github.com:EmerIlesS/bd_almacenamiento-masivo.git
cd bd_almacenamiento-masivo
python -m venv .venv
.\.venv\Scripts\activate
pip install -r requirements.txt
```

### 2. Configurar Variables de Entorno
Crea un archivo `.env` en la raíz (usando de base `.env.example`):
```env
MONGO_URI="mongodb+srv://<usuario>:<password>@cluster0.xxxxx.mongodb.net/?retryWrites=true&w=majority"
MONGO_DB_NAME="hyperflix_db"
```

### 3. Verificar Conexión a MongoDB Atlas
```powershell
python conexion.py
```

### 4. Inicializar Estructura e Índices en Atlas
Crea las colecciones OLTP (`usuarios`, `suscripciones`, `peliculas`, `canales_tv`, `pagos`, `eventos_reproduccion`), la colección OLAP (`reproducciones_analiticas`) y sus respectivos índices únicos y de búsqueda:
```powershell
python scripts/crear_estructura.py
```

### 5. Generar Datos Masivos Realistas (20.000+ Eventos)
Genera más de 1.500 usuarios, 66 películas, canales IPTV y más de 20.000 eventos de streaming simulando comportamientos de abandono, pausas, saltos y métricas de QoS:
```powershell
python scripts/generar_datos_masivos.py
```

### 6. Transformación OLTP a OLAP (Modelo en Estrella)
Ejecuta el pipeline de desnormalización que construye la tabla de hechos `FACT_REPRODUCCIONES` y dimensiones en `reproducciones_analiticas`:
```powershell
python scripts/transformar_oltp_a_olap.py
```

### 7. Benchmarking de 10 Consultas Analíticas Optimizadas (OLAP) ⭐
Evalúa el rendimiento de 10 consultas analíticas complejas sobre el modelo dimensional desnormalizado (`reproducciones_analiticas`) con índices compuestos selectivos y `allowDiskUse: true`, garantizando latencias bajo el SLA (< 500 ms):
```powershell
python scripts/benchmarking_consultas.py
```
*(Promedio real obtenido: ~243 ms sobre más de 20.000 eventos analíticos).*

### 8. Ejecutar el Pipeline del Data Lake (Parquet + Snappy) ⭐
Ejecuta el pipeline oficial que extrae de Atlas, transforma con Pandas y genera los archivos en las 3 zonas (`raw`, `processed`, `curated`):
```powershell
python data_lake.py
```

### 9. Demostración de Event Streaming & CDC en Tiempo Real (Simulación Kafka + Spark Streaming) ⚡
Abre **dos terminales de PowerShell en paralelo** para demostrar el flujo en vivo:
- **Terminal 1 (Consumidor Spark Streaming / Change Streams):**
  ```powershell
  python scripts/consumidor_streaming_cdc.py
  ```
- **Terminal 2 (Productor de Telemetría / Emisor Kafka):**
  ```powershell
  python scripts/productor_streaming.py
  ```
*(Observarás cómo cada evento emitido en la Terminal 2 es capturado instantáneamente por el Change Stream en la Terminal 1 sin bloqueos ni polling).*

### 10. Motor de Procesamiento Big Data con Apache Spark (Fase 2) ⚡
Procesa los datos masivos almacenados en el Data Lake implementando la arquitectura:
$$\text{MongoDB Atlas} \longrightarrow \text{Data Lake (Raw)} \longrightarrow \mathbf{\text{Apache Spark Job}} \longrightarrow \text{Curated / Data Warehouse} \longrightarrow \text{BI}$$

Cumple con todos los requisitos de la **Fase 2**:
1. **Configuración de Apache Spark (PySpark):** Sesión distribuida con `SparkSession`.
2. **Detección Dinámica del Último Archivo:** Selecciona automáticamente el lote más reciente generado en el Data Lake (*"En una empresa, cada día se genera un nuevo archivo. El programa siempre trabajará con el último"*).
3. **Transformación 1 (`explode`):** Desanida listas y arrays en múltiples filas individuales (*"Spark convierte una lista en varias filas"*).
4. **Transformación 2 (`withColumn`):** Limpieza, filtros, estandarización y categorizaciones de negocio (duración, satisfacción, marcas de auditoría).
5. **Transformación 3 (`groupBy` + `agg`):** Agregaciones Big Data para generar la tabla de hechos con KPIs analíticos.
6. **Destino Data Warehouse:** Guarda los resultados en `data_lake/curated/` en formato Apache Parquet.

**Comando de ejecución directa:**
```powershell
python spark_processor.py
```
*O procesando un archivo específico:*
```powershell
python spark_processor.py data_lake/raw/peliculas.parquet
```

### 11. Data Warehouse con Modelo en Estrella en MongoDB Atlas (Fase 3) ⭐
Transforma los datos analíticos de streaming en un **Modelo en Estrella (Star Schema)** físico en MongoDB Atlas, aplicando los conceptos de modelado dimensional que se usan en BigQuery, Snowflake y Redshift:

* **`dim_tiempo`:** Fecha, año, mes, día, hora pico y **trimestre calculado con `$ceil` y `$divide`**, más fin de semana con **`$cond`**.
* **`dim_usuario`:** Identificador único, país, ciudad, plan, horas vistas y promedio por sesión con **`$first`**, **`$sum`** y **`$avg`**.
* **`dim_contenido`:** Títulos VOD / canales IPTV, género/categoría, minutos reproducidos y completitud media con **`$first`** y **`$round`**.
* **`dim_dispositivo`:** Smart TVs, móviles, PCs y métricas de Calidad de Servicio (QoS).
* **`fact_reproducciones`:** **Tabla de hechos central**, almacena únicamente las claves foráneas a las dimensiones (`fecha_id`, `usuario_id`, `contenido_id`, `dispositivo_tipo`) junto a las métricas cuantitativas, eliminando la duplicación de datos.
* **Optimización Big Data:** Ejecutado con **`allowDiskUse=True`** y persistencia directa mediante **`$merge`**.

**Comando de ejecución (HyperFlix Streaming):**
```powershell
python data_warehouse.py
```

*(Opcional: Si deseas ejecutar el caso de estudio de clase sobre e-commerce/ventas: `python data_warehouse.py ventas`).*

### 12. Generar el Dashboard de Monitoreo Integral (Métricas, Latencias y Costos) ⭐
Genera un panel visual de 6 gráficos de alta resolución (`dashboard_monitoreo_latest.png`) y un reporte ejecutivo en consola que audita en tiempo real:
1. **Volumen de Documentos por Colección** (OLTP, OLAP, Star Schema).
2. **Cumplimiento de SLA de Latencia** (< 500 ms) en las 10 consultas analíticas.
3. **Estimación de Costos Mensuales de Infraestructura Cloud** (MongoDB Atlas M10 + Almacenamiento).
4. **Tendencia de Streaming Mensual** (Evolución de horas vistas y sesiones).
5. **Métricas de Capacidad y Almacenamiento** (Normalizadas en GB y proyección temporal).
6. **Resumen Ejecutivo de Disponibilidad del Sistema** (KPIs de retención, latencia media y estado operativo).

```powershell
python dashboard.py
```
*(O directamente: `python scripts/dashboard_monitoreo.py`).*

### 13. Automatización de Infraestructura como Código (IaC / Terraform) ⭐
Audita el clúster multi-región en MongoDB Atlas, valida las particiones del Data Lake Parquet, verifica los parámetros de Disaster Recovery y exporta el manifiesto oficial del estado de la infraestructura (`infraestructura_estado.json`):

```powershell
python infraestructura.py
```
*(Para simular la salida de un plan de Terraform ante el docente: `python infraestructura.py --plan`).*

Los módulos declarativos en sintaxis HCL se encuentran listos en el directorio [`terraform/`](terraform/) (`main.tf`, `variables.tf`, `outputs.tf`).

### 14. Lanzar la Plataforma Web & Reproductor IPTV en Vivo
Abre directamente `web/index.html` en tu navegador, o inicia un servidor local:
```powershell
python -m http.server 8000 --directory web
```
Luego ingresa en tu navegador a: `http://localhost:8000`.

---

## 📊 Las 10 Consultas Analíticas Evaluadas en Benchmarking
Todas las consultas se ejecutan con `allowDiskUse: true` e índices compuestos dedicados:
1. **Series Temporales de Streaming Mensual:** Evolución histórica de horas vistas y reproducciones por mes.
2. **Top 5 Contenidos con Mayor Engagement por Género:** Ranking de películas/series líderes en horas vistas y completitud.
3. **Rendimiento de Audiencia por Segmento de Usuario y Ciudad:** Análisis geográfico de retención.
4. **Consumo Promedio y Volumen por Plan de Suscripción:** Comparativa Premium vs Estándar vs Básico.
5. **Tendencia de Streaming según Día de la Semana:** Análisis de picos de fin de semana vs días laborales.
6. **Tasa de Abandono (Drop-off Rate) por Tipo de Dispositivo:** Diagnóstico de fallos en Smart TVs, Web y Móviles.
7. **Top 10 Usuarios VIP / Super-Streamers:** Clientes de mayor fidelidad y horas reproducidas acumuladas.
8. **Reproducciones Trimestrales (Estacionalidad Q1-Q4):** Comportamiento estacional por trimestres con `$ceil` y `$divide`.
9. **Tiempo Medio de Sesión por Ciudad y Segmento:** Análisis de duración de sesión en las 15 principales ciudades.
10. **Top 10 Contenidos con Mayor Retención / Completitud (%):** Eficacia del contenido midiendo el porcentaje visto.

---

## 📄 Documentación Técnica Completa
- 📘 **Informe Oficial del Parcial (Primer Corte):** [docs/INFORME_PARCIAL_HYPERFLIX.md](docs/INFORME_PARCIAL_HYPERFLIX.md)
- 📗 **Diseño de Arquitectura General (Semanas 1 y 2):** [docs/ARQUITECTURA_HYPERFLIX.md](docs/ARQUITECTURA_HYPERFLIX.md)
- 🛡️ **Estrategia de Disaster Recovery Cross-Region (DR):** [docs/DISASTER_RECOVERY_CROSS_REGION.md](docs/DISASTER_RECOVERY_CROSS_REGION.md)
- 🏗️ **Infraestructura como Código (IaC / Terraform):** [docs/INFRAESTRUCTURA_COMO_CODIGO_IAC.md](docs/INFRAESTRUCTURA_COMO_CODIGO_IAC.md)
