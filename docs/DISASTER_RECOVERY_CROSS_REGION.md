# 🛡️ HYPERFLIX — Estrategia de Disaster Recovery Cross-Region (DR)
**Materia:** Bases de Datos y Almacenamiento Masivo (Octavo Semestre)  
**Programa:** Ingeniería de Sistemas — Institucion  universitaria del putumayo  
**Proyecto:** HYPERFLIX — Plataforma de Streaming, VOD, Canales IPTV y Telemetría Masiva  
**Estándar:** ISO 22301 (Continuidad de Negocio) y AWS Well-Architected Reliability Pillar  

---

## 📌 1. Introducción y Definición del Requisito

En arquitecturas de almacenamiento masivo y streaming que procesan más de **20.000.000 de eventos diarios (52 GB/día)** procedentes de **1.000.000 de usuarios activos**, confiar en un único centro de datos o una única región de nube constituye un **Punto Único de Falla Catastrófico (SPOF - Single Point of Failure)**.

### ¿Qué es Disaster Recovery (DR) Cross-Region?
Una estrategia de **Disaster Recovery (DR) Cross-Region** es el conjunto articulado de políticas, arquitecturas tecnológicas, réplicas asíncronas de datos y procedimientos de conmutación por error (*failover*) diseñados para garantizar que la plataforma sobreviva a fallos totales de infraestructura geográfica (cortes de fibra continental, caídas masivas de centros de datos por desastres naturales, fallas eléctricas regionales de la nube o ataques dirigidos de ransomware), transfiriendo la operación a una **región secundaria geográfica distante** sin interrupciones severas ni pérdida inaceptable de datos.

---

## 🎯 2. Métricas Críticas de Continuidad de Negocio: RPO y RTO

Para el ecosistema **HYPERFLIX**, se han establecido los siguientes acuerdos de nivel de servicio (SLA) de recuperación:

```
┌────────────────────────────────────────────────────────────────────────────────────────┐
│                        LÍNEA DE TIEMPO ANTE UN DESASTRE REGIONAL                       │
│                                                                                        │
│                                   Momento del Desastre                                 │
│                                            ▼                                           │
│   ───[ Datos Replicados ]──────[ Pérdida ]─┼──[ Caída / Detección ]──[ Restauración ]──  │
│   ◄───────────────────────►    ◄─────────► │  ◄──────────────────────────────────►     │
│       Operación Normal             RPO     │                 RTO                       │
│                                (< 1 min)   │              (< 5 min)                    │
└────────────────────────────────────────────────────────────────────────────────────────┘
```

| Métrica | Definición | Valor Meta HYPERFLIX | Justificación Técnica en Streaming |
|---|---|---|---|
| **RPO (Recovery Point Objective)** | Cantidad máxima tolerable de datos que pueden perderse medido en tiempo transcurrido. | **$< 1$ minuto** (Transacciones / Usuarios / Pagos)<br>**$< 5$ minutos** (Telemetría de tele-reproducción) | Los pagos y perfiles de usuario se replican casi en tiempo real vía el Oplog de MongoDB Atlas. Para telemetría masiva de streaming, los reproductores web/móviles retienen un búfer local de eventos de 5 minutos en IndexedDB/LocalCache. |
| **RTO (Recovery Time Objective)** | Tiempo máximo admisible para restaurar el servicio y recuperar la operatividad completa tras el incidente. | **$< 5$ minutos** (Capa de Base de Datos y API)<br>**$< 15$ minutos** (Pipelines Analíticos y PySpark) | El clúster de base de datos ejecuta conmutación automática (*Automatic Failover*) por consenso Raft en segundos. El tráfico de usuarios se desvía vía DNS Anycast (Cloudflare / Route 53) en menos de 2 minutos. |
| **MTTR (Mean Time to Recover)** | Tiempo promedio que toma al equipo de ingeniería diagnosticar y devolver el clúster principal a la normalidad. | **$< 2$ horas** | Aislamiento automatizado de la zona dañada y restauración desde snapshots inmutables con Point-in-Time Recovery (PITR). |
| **MTBF (Mean Time Between Failures)** | Tiempo promedio esperado entre incidentes críticos de infraestructura. | **$> 8.760$ horas (99.99% anual)** | Diseño activo-pasivo cálido (*Warm Standby*) con redundancia N+2. |

---

## 🗺️ 3. Diagrama de Arquitectura Cross-Region (Multi-Region)

La arquitectura de **HYPERFLIX** despliega dos regiones geográficamente desacopladas:
- **Región Primaria (Activa):** AWS `us-east-1` (Norte de Virginia). Procesa el 100% de la carga de producción en condiciones normales.
- **Región Secundaria / DR (Warm Standby):** AWS `us-west-2` (Oregón) o GCP `southamerica-east1` (São Paulo). Mantiene copias replicadas de bases de datos, almacenamiento Parquet sincronizado y capacidad de cómputo en reserva.

```mermaid
flowchart TB
    subgraph Usuarios["Dispositivos & Reproductores Globales"]
        CLIENTS["1.000.000 Usuarios (Web, Android, iOS, Smart TV)"]
    end

    subgraph Trafico["Gestión Global de Tráfico & DNS Failover"]
        DNS["Route 53 / Cloudflare Traffic Steering\n(Health Checks activos cada 10s)"]
    end

    subgraph RegionPrimaria["REGIÓN PRIMARIA (us-east-1) - ACTIVA"]
        direction TB
        subgraph Ingesta1["Ingesta & API Primaria"]
            API1["API Gateway NestJS (Auto-scaling)"]
            KAFKA1["Apache Kafka Cluster (Primario)"]
        end

        subgraph BD1["Persistencia Transaccional & OLAP (Atlas)"]
            NODE1["Atlas Node 1: PRIMARY\n(Lectura / Escritura)"]
            NODE2["Atlas Node 2: SECONDARY\n(Votante / Réplica local)"]
        end

        subgraph Lake1["Data Lake Parquet (Primario)"]
            S3_1["S3 Primary Bucket (us-east-1)\n- raw/\n- processed/\n- curated/"]
        end

        subgraph Compute1["Cómputo Distribuido"]
            SPARK1["Apache Spark Cluster (ETL Diario)"]
        end
    end

    subgraph RegionDR["REGIÓN SECUNDARIA DE DESASTRE (us-west-2) - WARM STANDBY"]
        direction TB
        subgraph Ingesta2["Ingesta & API Reserva"]
            API2["API Gateway NestJS (Pilot Light / Min Scale)"]
            KAFKA2["Apache Kafka Cluster (Standby / MirrorMaker)"]
        end

        subgraph BD2["Persistencia DR (Atlas Multi-Region)"]
            NODE3["Atlas Node 3: SECONDARY ELECTABLE\n(Prioridad 6 - Oplog Sync continuo)"]
            NODE4["Atlas Node 4: READ-ONLY ANALYTICS\n(Reportes & BI de respaldo)"]
        end

        subgraph Lake2["Data Lake Parquet (Copia Espejo DR)"]
            S3_2["S3 DR Bucket (us-west-2)\n- Versioning Activo\n- Object Lock WORM"]
        end

        subgraph Compute2["Cómputo en Espera"]
            SPARK2["PySpark Worker Pool (IaC Terraform On-Demand)"]
        end
    end

    %% Enrutamiento y monitoreo
    CLIENTS -->|Tráfico HTTPS / WSS| DNS
    DNS -->|Tráfico Normal (99.99%)| API1
    DNS -.->|FAILOVER AUTOMÁTICO ante Caída Regional| API2

    %% Flujos internos Primarios
    API1 --> KAFKA1
    API1 --> NODE1
    KAFKA1 --> Lake1
    Lake1 --> SPARK1

    %% SINCRONIZACIÓN CROSS-REGION (DISASTER RECOVERY)
    NODE1 ==>|1. Replicación continua de Oplog (< 2 segs)| NODE3
    NODE1 ==>|Snapshots Automáticos + PITR (7 días)| NODE4
    S3_1 ==>|2. S3 Cross-Region Replication (CRR Asíncrona)| S3_2
    KAFKA1 ==>|3. MirrorMaker 2 / Cluster Linking (Tópicos de Telemetría)| KAFKA2

    %% Activación en DR
    API2 -.->|En Desastre: Promovido a PRIMARY| NODE3
    API2 -.-> KAFKA2
    Lake2 -.-> SPARK2
```

---

## 🧱 4. Estrategia de Recuperación por Capa Tecnológica

### 4.1. Capa de Base de Datos: MongoDB Atlas Multi-Region Replica Set
1. **Topología Distribuida Geográficamente:**
   - La base de datos `hyperflix_db` no se aloja en un único centro de datos. Se configura un **Multi-Region Replica Set de 3 a 5 nodos**:
     - **2 Nodos en `us-east-1`:** 1 Nodo Primario (Prioridad 7) y 1 Nodo Secundario (Prioridad 7).
     - **1 Nodo Secundario Electable en `us-west-2`:** (Prioridad 6).
     - **1 Nodo Read-Only / Analítico en `us-west-2`:** Para no impactar las lecturas operacionales durante la replicación transcontinental.
2. **Replicación Asíncrona del Oplog:**
   - Cada inserción en colecciones transaccionales (`usuarios`, `suscripciones`, `pagos`, `peliculas`, `canales_tv`) y analíticas (`reproducciones_analiticas`, `fact_reproducciones`) se escribe en el *Operations Log (Oplog)* del nodo primario y se transmite por la red privada dedicada de AWS hacia el nodo en `us-west-2` con latencias de transmisión de **$\approx 30-50$ ms**.
3. **Continuous Cloud Backups & Point-in-Time Recovery (PITR):**
   - **Snapshots Programados:** Copias de seguridad completas cada 6 horas conservadas durante 30 días en una región de respaldo aislada.
   - **PITR (Ventana de 7 días):** Capacidad de restaurar la base de datos a cualquier segundo exacto en los últimos 7 días ante corrupciones lógicas masivas, ataques cibernéticos o errores humanos de migración.
4. **Conmutación Automática por Consenso Raft:**
   - Si la región `us-east-1` sufre una caída total, el nodo en `us-west-2` detecta la pérdida de latidos de corazón (*heartbeats*) en menos de **10 segundos**, asume quórum con los nodos supervivientes y se auto-promueve a **PRIMARY**, restableciendo operaciones de escritura sin intervención humana.

---

### 4.2. Capa de Almacenamiento Masivo: Data Lake Parquet con Cross-Region Replication (CRR)
1. **Zonas del Data Lake Replicadas (`raw`, `processed`, `curated`):**
   - El almacenamiento columnar de archivos Parquet con compresión Snappy (`usuarios.parquet`, `peliculas.parquet`, `canales_tv.parquet`, `eventos_reproduccion.parquet`, `kpis_spark.parquet`) se almacena en el bucket primario `s3://hyperflix-datalake-primary-us-east-1/`.
2. **S3 Cross-Region Replication (CRR):**
   - Se habilita replicación automática asíncrona de bucket a bucket hacia `s3://hyperflix-datalake-dr-us-west-2/`.
   - Tan pronto como el pipeline de Python (`data_lake.py`) escribe un nuevo lote Parquet en la región primaria, AWS S3 replica el archivo en la región secundaria en un promedio de **$< 15$ minutos**.
3. **Inmutabilidad y Protección Anti-Ransomware:**
   - **Object Lock (WORM - Write Once, Read Many):** Los archivos curados de telemetría y tablas de hechos quedan bloqueados por 90 días, impidiendo su alteración o eliminación incluso por credenciales administrativas comprometidas.
   - **S3 Bucket Versioning:** Mantiene versiones históricas de cada archivo Parquet generado por lotes diarios de streaming.

---

### 4.3. Capa de Ingesta y Streaming: Apache Kafka MirrorMaker 2
1. **Sincronización de Tópicos de Telemetría:**
   - Para evitar perder eventos de telemetría en tránsito (`hyperflix.playback.events`, `hyperflix.telemetry.heartbeat`), se implementa **Apache Kafka MirrorMaker 2 (MM2)**.
   - MM2 replica activamente los registros y mantiene alineados los *consumer offsets* entre el clúster de Kafka en `us-east-1` y el clúster en `us-west-2`.
2. **Búfer de Resiliencia en el Cliente (Client-Side Circuit Breaker):**
   - En el frontend web (`web/app.js`) y reproductores de Smart TV, el emisor de telemetría implementa almacenamiento local temporal (*Local Cache / IndexedDB*). Si el backend primario deja de responder, el cliente acumula los eventos de QoS y los despacha en ráfaga (*flush*) una vez que el DNS redirige el tráfico a la región DR.

---

### 4.4. Capa de Enrutamiento Global: DNS Failover con Health Checks
1. **Supervisión de Salud Automatizada:**
   - El servicio de gestión de tráfico DNS (AWS Route 53 / Cloudflare Anycast) ejecuta **Health Checks cada 10 segundos** al endpoint `/health` del API Gateway primario.
   - **Criterio de Fallo:** Si 3 verificaciones consecutivas devuelven un código HTTP $\neq 200$ o exceden un timeout de 4.000 ms (lapso de 30 segundos), el sistema declara la región primaria en estado **UNHEALTHY**.
2. **Conmutación Transparente de Clientes:**
   - Route 53 actualiza dinámicamente los registros de dirección A/AAAA con un **TTL ultra-bajo de 60 segundos**, redirigiendo las peticiones de los 1.000.000 de clientes hacia las IPs del API Gateway de la región `us-west-2`.

---

### 4.5. Capa de Cómputo Big Data: PySpark On-Demand con Infraestructura como Código (IaC)
1. **Aprovisionamiento Elástico con Terraform:**
   - Mantener un clúster masivo de Apache Spark encendido 24/7 en la región de desastre generaría costos innecesarios de miles de dólares al mes.
   - En su lugar, el pipeline Big Data (`spark_processor.py` y `scripts/spark_job.py`) utiliza plantillas de **Terraform** que aprovisionan clústeres efímeros de PySpark en la región DR bajo demanda cuando se declara la emergencia, apuntando directamente al bucket S3 replicado en `us-west-2`.

---

## 📊 5. Clasificación del Modelo DR: *Warm Standby* (Híbrido Eficiente)

En la industria de datos existen 4 estrategias estándar de Disaster Recovery:

```
Costos ($)
  ▲
  │                                                   [Active-Active Multi-Site]
  │                                                   (Costos ×2, RTO ~ 0s)
  │                                      [WARM STANDBY - HYPERFLIX]
  │                                      (BD Hot + Cómputo On-Demand, RTO < 5m)
  │                         [Pilot Light]
  │                         (BD mínima, RTO ~ 30m)
  │            [Backup & Restore]
  │            (RTO horas / días)
  └─────────────────────────────────────────────────────────────────────────────► Complejidad / SLA
```

### Justificación de la Elección para HYPERFLIX:
HYPERFLIX implementa **Warm Standby**:
- La **capa de datos transaccionales y analíticos (MongoDB Atlas)** opera en modo **Hot Standby** (nodos vivos y sincronizados en tiempo real para no perder sesiones ni pagos de usuarios).
- La **capa de ingesta y API** opera en modo **Pilot Light** (instancias mínimas escalables en segundos).
- La **capa de procesamiento masivo (Apache Spark)** opera en modo **On-Demand** mediante IaC.
- **Beneficio:** Brinda un RTO $< 5$ minutos y un RPO $< 1$ minuto con un incremento presupuestario de tan solo un **35% sobre la infraestructura base**, a diferencia de un esquema Activo-Activo que duplicaría el 100% de los costos de servidores.

---

## 🔄 6. Protocolo Operacional de Conmutación (Runbook de Failover & Failback)

### Fase A: Detección y Declaración del Desastre (Minuto 0 al 2)
1. **Alerta Crítica:** PagerDuty / Datadog recibe alertas de caída masiva en `us-east-1` (tasa de error $> 50\%$ en la API Gateway o pérdida de conectividad con Atlas).
2. **Evaluación Automática:** Los Health Checks de Route 53 confirman fallo en la región primaria durante 3 ciclos (30 segundos).
3. **Declaración:** Se dispara el incidente de severidad SEV-1 (Regional Disaster Outage).

### Fase B: Ejecución del Failover (Minuto 2 al 5)
1. **Promoción de Base de Datos:** MongoDB Atlas detecta la partición de red y el nodo de `us-west-2` gana la elección de Primario.
2. **Redirección de Tráfico:** Route 53 conmuta el tráfico DNS global apuntando a los balanceadores de carga de `us-west-2`.
3. **Auto-escalado de API:** Las instancias de API Gateway en `us-west-2` escalan automáticamente de 2 a 20 instancias para absorber la totalidad del tráfico de streaming.
4. **Activación de Consumidores:** Los servicios de ingesta apuntan a los tópicos locales de Kafka en `us-west-2` con offsets replicados por MirrorMaker 2.
5. **Verificación de Reproducción:** Los clientes web y de Smart TV continúan reproduciendo catálogo VOD y canales IPTV sin cerrar sesión.

### Fase C: Failback (Retorno Controlado a la Región Primaria)
Una vez que el proveedor de nube certifica la total normalización de `us-east-1`:
1. **Sincronización Inversa:** Se habilita la replicación en sentido inverso (`us-west-2` ➔ `us-east-1`) para transferir todos los eventos y escrituras ocurridas durante la contingencia.
2. **Drenado de Conexiones (Canary Shifting):** Se desvía el 10% del tráfico a `us-east-1` mediante pesos DNS para verificar estabilidad.
3. **Conmutación Final:** Al validar 0 errores durante 15 minutos, se conmuta el 100% del tráfico de regreso y se degrada el nodo de `us-west-2` a secundario.

---

## 🧪 7. Plan de Pruebas y Simulacros de Desastre (Chaos Engineering & Game Days)

La estrategia de Disaster Recovery es evaluada semestralmente bajo las siguientes pruebas:

1. **Simulacro de Partición de Red (Chaos Kong / Chaos Mesh):**
   - Se inyecta una regla de bloqueo de red que aísla por completo los nodos primarios en `us-east-1`.
   - **Criterio de Aprobación:** Elección de nuevo primario en $< 10$ segundos y reanudación de inserciones en `reproducciones_analiticas` sin pérdida de registros.
2. **Prueba de Resiliencia del Data Lake:**
   - Se simula la corrupción inducida del bucket principal de Parquet.
   - **Criterio de Aprobación:** Ejecución exitosa del script `spark_processor.py` leyendo los metadatos desde el bucket réplica en `us-west-2` en menos de 10 minutos.
3. **Auditoría de Integridad del Oplog:**
   - Verificación de consistencia cruzada mediante hash SHA-256 entre documentos de `usuarios` y `reproducciones_analiticas` en ambas regiones.

---

## 📋 8. Matriz Resumen de Cumplimiento Técnico

| Dimensión | Requisito Académico y de Industria | Implementación Oficial en HYPERFLIX |
|---|---|---|
| **Distribución Geográfica** | Al menos 2 regiones desacopladas físicamente. | AWS `us-east-1` (Primaria) + AWS `us-west-2` (DR Standby). |
| **Pérdida Máxima de Datos (RPO)** | Definición rigurosa de límites de pérdida. | RPO $< 1$ min (Transacciones y cuentas) / RPO $< 5$ min (Telemetría QoS). |
| **Tiempo de Recuperación (RTO)** | Recuperación en tiempo aceptable de negocio. | RTO $< 5$ min (Failover automatizado sin intervención manual). |
| **Mecanismo de Replicación BD** | Evitar copias manuales periódicas. | Replicación continua de Oplog en MongoDB Atlas Multi-Region Replica Set. |
| **Protección del Data Lake** | Garantía de disponibilidad de archivos Parquet. | Cross-Region Replication (CRR) en S3 + Inmutabilidad WORM (Object Lock). |
| **Resiliencia de Ingesta** | No perder eventos masivos de reproducción. | Kafka MirrorMaker 2 + Búfer de resiliencia local en el frontend web. |
| **Enrutamiento** | Conmutación desasistida de clientes. | AWS Route 53 / Cloudflare Health Checks con DNS Failover en 60 segundos. |
