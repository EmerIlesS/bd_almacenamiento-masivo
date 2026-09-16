# 🎬 HYPERFLIX — Arquitectura de Datos, Streaming Masivo & Data Lake

> **Proyecto de Bases de Datos y Almacenamiento Masivo (Octavo Semestre)**  
> **Fase 1 y Fase 2: Análisis de Requisitos, Persistencia Políglota y Data Lake Parquet**  
> **Programa:** Ingeniería de Sistemas — ITP (2026-1)

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
├── requirements.txt                  # Dependencias de Python (pymongo, pandas, pyarrow, faker, dotenv)
├── README.md                         # Guía general de uso y presentación del proyecto
├── docs/
│   ├── INFORME_PARCIAL_HYPERFLIX.md  # Informe técnico oficial del Parcial Primer Corte (Adaptado a HyperFlix)
│   └── ARQUITECTURA_HYPERFLIX.md     # Documento maestro con especificación formal y diagramas Mermaid
├── scripts/
│   ├── crear_estructura.py           # Inicializa colecciones OLTP, índices únicos y datos semilla
│   ├── generar_datos_masivos.py      # Generador de 20.000+ eventos de streaming, 1.500+ usuarios y catálogo
│   ├── transformar_oltp_a_olap.py    # Pipeline ETL de agregación para construir el Modelo en Estrella
│   └── benchmarking_consultas.py     # Benchmarking analítico con medición de latencias (ms) e índices compuestos
├── data_lake/                        # Data Lake estructurado en 3 zonas (Formato Parquet Snappy)
│   ├── raw/                          # usuarios.parquet, peliculas.parquet, canales_tv.parquet, eventos_reproduccion.parquet
│   ├── processed/                    # reproducciones_procesadas.parquet (Datos limpios y enriquecidos con fechas)
│   └── curated/                      # reproducciones_curadas.parquet (Modelo Estrella listo para BI)
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

### 7. Benchmarking de Consultas Analíticas
Evalúa el rendimiento de las 6 consultas analíticas con índices compuestos verificando que las latencias sean $< 500$ ms (promedio real: ~222 ms):
```powershell
python scripts/benchmarking_consultas.py
```

### 8. Ejecutar el Pipeline del Data Lake (Parquet + Snappy) ⭐
Ejecuta el pipeline oficial que extrae de Atlas, transforma con Pandas y genera los archivos en las 3 zonas (`raw`, `processed`, `curated`):
```powershell
python data_lake.py
```

### 9. Lanzar la Plataforma Web & Reproductor IPTV en Vivo
Abre directamente `web/index.html` en tu navegador, o inicia un servidor local:
```powershell
python -m http.server 8000 --directory web
```
Luego ingresa en tu navegador a: `http://localhost:8000`.

---

## 📊 Consultas Analíticas Evaluadas en Benchmarking
1. **Top 10 Películas más reproducidas y tiempo total consumido** (Latencia: ~500 ms).
2. **Tasa de Abandono (Drop-off Rate) por Género de Película** (Latencia: ~208 ms).
3. **Calidad de Servicio (QoS): Buffering, Bitrate y Latencia por Dispositivo y SO** (Latencia: ~150 ms).
4. **Distribución de Audiencia y Horas Vistas por País y Plan de Suscripción** (Latencia: ~163 ms).
5. **Análisis de Horas Pico y Picos de Carga (Stress Analysis)** (Latencia: ~162 ms).
6. **Comparativa de Consumo: Películas VOD vs Canales de TV en Vivo IPTV** (Latencia: ~145 ms).

---

## 📄 Documentación Técnica Completa
- 📘 **Informe Oficial del Parcial (Primer Corte):** [docs/INFORME_PARCIAL_HYPERFLIX.md](docs/INFORME_PARCIAL_HYPERFLIX.md)
- 📗 **Diseño de Arquitectura General (Semanas 1 y 2):** [docs/ARQUITECTURA_HYPERFLIX.md](docs/ARQUITECTURA_HYPERFLIX.md)
