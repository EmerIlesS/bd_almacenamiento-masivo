# ⚙️ Módulos de Infraestructura como Código (IaC) con Terraform — HYPERFLIX

Este directorio contiene la definición formal y declarativa de la infraestructura en la nube para la plataforma de streaming **HYPERFLIX**, aprovisionando de manera automatizada:

1. **MongoDB Atlas Multi-Región:** Clúster M10 con tolerancia a fallos de región (2 nodos en `us-east-1` + 1 nodo en `us-west-2` para Disaster Recovery con failover automático Raft).
2. **AWS S3 Data Lake Multi-Región:** Buckets en formato columnar Parquet con **Cross-Region Replication (CRR)** asíncrona entre `us-east-1` y `us-west-2`, versionado activo e inmutabilidad Object Lock.
3. **Firewall & Seguridad:** Gestión de usuarios de base de datos (`readWriteAnyDatabase`) y reglas de acceso IP autorizadas.

---

## 🚀 Comandos de Despliegue con Terraform

### 1. Inicializar Proveedores
Descarga los plugins oficiales de `mongodbatlas` y `aws`:
```bash
terraform init
```

### 2. Planificar el Despliegue (Dry-Run)
Verifica los recursos que se van a crear sin modificar nada en la nube:
```bash
terraform plan
```

### 3. Aprovisionar la Infraestructura
Crea y configura los clústeres y buckets automáticamente:
```bash
terraform apply -auto-approve
```

### 4. Consultar Outputs (Cadenas de Conexión y Buckets)
```bash
terraform output
```

### 5. Destruir Recursos (Al finalizar el ciclo de vida)
```bash
terraform destroy
```

---

## 🤖 Automatización sin instalar Terraform CLI
Si no tienes el binario de Terraform instalado localmente, puedes usar el script de automatización en Python provisto en el proyecto:
```bash
python scripts/automatizar_infraestructura.py
```
O desde la raíz:
```bash
python infraestructura.py
```
