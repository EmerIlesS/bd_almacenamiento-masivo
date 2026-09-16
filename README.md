# 🎬 HYPERFLIX — Arquitectura de Datos y Streaming Masivo

> **Proyecto Parcial I — Semanas 1 y 2: Análisis de Requisitos y Diseño de Arquitectura**  
> **Asignatura:** Bases de Datos y Almacenamiento Masivo (Octavo Semestre)  
> **Programa:** Ingeniería de Sistemas — ITP (2026-1)

---

## 📌 Descripción General

**HYPERFLIX** es una plataforma de streaming y análisis de datos a gran escala. Su objetivo es capturar y procesar de forma continua la telemetría y el comportamiento de visualización de **1.000.000 de usuarios** (generando más de **20.000.000 de eventos diarios / 52 GB al día**), alimentando modelos de Machine Learning (Spark MLlib) para recomendaciones personalizadas de baja latencia mediante Redis.

Incluye una **Landing Page interactiva**, catálogo de películas bajo demanda (VOD), reproductor en vivo de **canales de televisión públicos y libres (IPTV)** vía streaming HLS y un **Monitor de Telemetría en Tiempo Real**.

---

## 🛠️ Pila Tecnológica de la Arquitectura

- **Ingesta:** API Gateway (NestJS) + Apache Kafka (tópicos particionados por `user_id`).
- **Persistencia Transaccional (OLTP):** PostgreSQL / MongoDB Atlas (Usuarios, Suscripciones, Catálogo, Pagos).
- **Persistencia de Alta Frecuencia:** Apache Cassandra (Telemetría de reproducción y chat en vivo).
- **Data Lake (Zonas Raw, Bronze, Silver, Gold):** MinIO (S3 compatible) + Formato Apache Parquet (Snappy).
- **Procesamiento Distribuido:** Apache Spark / PySpark (ETL/ELT y MLlib).
- **Analítica OLAP & BI:** ClickHouse (Modelo en Estrella) + Apache Superset.
- **Caché y Recomendaciones:** Redis (Latencia de entrega < 5 ms).
- **Frontend:** HTML5, Tailwind CSS, JavaScript ES6, HLS.js.

---

## 📁 Estructura del Proyecto

```text
parcial_!/
├── .env                              # Configuración de variables de entorno (MongoDB Atlas URI)
├── conexion.py                       # Conexión central y utilidades de base de datos
├── requirements.txt                  # Dependencias de Python (pymongo, faker, dotenv, dnspython)
├── README.md                         # Guía general de uso y presentación del proyecto
├── docs/
│   └── ARQUITECTURA_HYPERFLIX.md    # Documento Maestro con especificación formal y diagramas Mermaid
├── scripts/
│   ├── crear_estructura.py           # Inicializa colecciones OLTP, índices y datos semilla
│   ├── generar_datos_masivos.py      # Generador masivo de telemetría, usuarios y películas
│   ├── transformar_oltp_a_olap.py    # Pipeline ETL/ELT para construir el Modelo en Estrella
│   ├── benchmarking_consultas.py     # Benchmarking analítico con medición de latencias (ms)
│   └── simular_data_lake.py          # Organización y particionamiento en las 4 zonas del Data Lake
├── data_lake/                        # Zonas del Data Lake
│   ├── raw/                          # Eventos crudos en JSON directo desde Kafka
│   ├── bronze/                       # Datos estructurados particionados por year=YYYY/month=MM/day=DD
│   ├── silver/                       # Datos limpios y enriquecidos listos para ML
│   └── gold/                         # Métricas agregadas y rankings de consumo para BI
└── web/                              # Aplicación Web y Landing Page
    ├── index.html                    # Landing page + Reproductor IPTV HLS + Monitor de Telemetría
    └── app.js                        # Lógica de reproducción, canales libres y telemetría en vivo
```

---

## 🚀 Guía Paso a Paso de Ejecución

### 1. Activar el Entorno Virtual e Instalar Dependencias
```powershell
.\.venv\Scripts\activate
pip install -r requirements.txt
```

### 2. Verificar la Conexión a MongoDB Atlas
```powershell
python conexion.py
```

### 3. Crear las Colecciones, Índices y Datos Semilla
Crea las colecciones OLTP (`usuarios`, `suscripciones`, `peliculas`, `canales_tv`, `pagos`, `eventos_reproduccion`), la colección OLAP (`reproducciones_analiticas`) y sus respectivos índices optimizados:
```powershell
python scripts/crear_estructura.py
```

### 4. Generar Datos Masivos Realistas
Genera más de 1.500 usuarios, 60+ películas, canales IPTV y más de 20.000 eventos de streaming simulando comportamientos de abandono, pausas, saltos y métricas de QoS:
```powershell
python scripts/generar_datos_masivos.py
```

### 5. Transformar de OLTP a OLAP (Pipeline de Agregación)
Ejecuta el pipeline de desnormalización que construye la tabla de hechos `FACT_REPRODUCCIONES` y dimensiones en `reproducciones_analiticas`:
```powershell
python scripts/transformar_oltp_a_olap.py
```

### 6. Ejecutar el Benchmarking Analítico
Evalúa el rendimiento de 6 consultas de negocio clave midiendo las latencias en milisegundos con índices compuestos:
```powershell
python scripts/benchmarking_consultas.py
```

### 7. Simular las Zonas del Data Lake
Organiza y exporta la información en las zonas `raw`, `bronze`, `silver` y `gold`:
```powershell
python scripts/simular_data_lake.py
```

### 8. Abrir la Plataforma Web & Reproductor IPTV en Vivo
Abre directamente `web/index.html` en tu navegador, o inicia un servidor local:
```powershell
python -m http.server 8000 --directory web
```
Luego ingresa en tu navegador a: `http://localhost:8000`.

---

## 📊 Consultas Analíticas Evaluadas en Benchmarking
1. **Top 10 Películas más reproducidas y tiempo total consumido.**
2. **Tasa de Abandono (Drop-off Rate) por Género de Película.**
3. **Calidad de Servicio (QoS): Buffering, Bitrate y Latencia por Dispositivo y SO.**
4. **Distribución de Audiencia y Horas Vistas por País y Plan de Suscripción.**
5. **Análisis de Franjas Horarias y Picos de Carga (Stress Analysis).**
6. **Comparativa de Consumo: Películas VOD vs Canales de TV en Vivo (IPTV).**

---

## 📄 Documentación Completa
Para una explicación académica detallada con justificaciones de persistencia políglota, cálculos de velocidad, volumen y variedad, y diagramas de flujo completos, consulta:
👉 **[docs/ARQUITECTURA_HYPERFLIX.md](docs/ARQUITECTURA_HYPERFLIX.md)**
