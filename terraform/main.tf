# =============================================================================
# HYPERFLIX - INFRAESTRUCTURA COMO CÓDIGO (IaC) - RECURSOS PRINCIPALES
# =============================================================================
# Plataforma: MongoDB Atlas Multi-Region + AWS S3 Data Lake (Cross-Region DR)
# =============================================================================

provider "mongodbatlas" {
  public_key  = var.atlas_public_key
  private_key = var.atlas_private_key
}

# Proveedor AWS Región Primaria (us-east-1)
provider "aws" {
  region = var.aws_primary_region
}

# Proveedor AWS Región Secundaria DR (us-west-2)
provider "aws" {
  alias  = "dr_region"
  region = var.aws_dr_region
}

# Generador de sufijo aleatorio para nombres únicos de buckets
resource "random_id" "bucket_suffix" {
  byte_length = 4
}

# -----------------------------------------------------------------------------
# 1. MONGODB ATLAS: PROYECTO & CLÚSTER MULTI-REGIÓN (DISASTER RECOVERY)
# -----------------------------------------------------------------------------

resource "mongodbatlas_project" "hyperflix_project" {
  name   = var.project_name
  org_id = var.atlas_org_id
}

# Clúster Multi-Región con tolerancia a fallos geográficos
resource "mongodbatlas_advanced_cluster" "hyperflix_cluster" {
  project_id   = mongodbatlas_project.hyperflix_project.id
  name         = var.cluster_name
  cluster_type = "REPLICASET"

  replication_specs {
    num_shards = 1

    # Región Primaria: US_EAST_1 (2 nodos electables + 1 analítico de lectura)
    region_configs {
      electable_specs {
        instance_size = var.cluster_tier
        node_count    = 2
      }
      analytics_specs {
        instance_size = var.cluster_tier
        node_count    = 1
      }
      provider_name = var.cloud_provider
      region_name   = var.primary_region
      priority      = 7
    }

    # Región Secundaria (Disaster Recovery): US_WEST_2 (1 nodo electable en Standby)
    region_configs {
      electable_specs {
        instance_size = var.cluster_tier
        node_count    = 1
      }
      provider_name = var.cloud_provider
      region_name   = var.dr_region
      priority      = 6 # Prioridad menor para conmutación automática ante fallo regional
    }
  }

  # Respaldos continuos en la nube con Point-in-Time Recovery (PITR)
  backup_enabled = true
}

# Usuario administrador de base de datos para la aplicación
resource "mongodbatlas_database_user" "db_user" {
  username           = var.db_username
  password           = var.db_password
  project_id         = mongodbatlas_project.hyperflix_project.id
  auth_database_name = "admin"

  roles {
    role_name     = "readWriteAnyDatabase"
    database_name = "admin"
  }

  scopes {
    name = mongodbatlas_advanced_cluster.hyperflix_cluster.name
    type = "CLUSTER"
  }
}

# Lista de control de acceso IP (Firewall de Atlas)
resource "mongodbatlas_project_ip_access_list" "ip_access" {
  count      = length(var.ip_whitelist)
  project_id = mongodbatlas_project.hyperflix_project.id
  cidr_block = var.ip_whitelist[count.index]
  comment    = "Regla de acceso IP autorizada para infraestructura de HyperFlix"
}

# -----------------------------------------------------------------------------
# 2. DATA LAKE EN AWS S3: MULTI-REGION CON CROSS-REGION REPLICATION (CRR)
# -----------------------------------------------------------------------------

# Bucket Primario en us-east-1 (Zonas raw, processed, curated)
resource "aws_s3_bucket" "datalake_primary" {
  bucket        = "hyperflix-datalake-primary-${random_id.bucket_suffix.hex}"
  force_destroy = false

  tags = {
    Project     = "HYPERFLIX"
    Environment = var.environment
    Layer       = "DataLake-Primary"
    Region      = var.aws_primary_region
  }
}

resource "aws_s3_bucket_versioning" "primary_versioning" {
  bucket = aws_s3_bucket.datalake_primary.id
  versioning_configuration {
    status = "Enabled"
  }
}

# Bucket Secundario en us-west-2 (Copia Espejo de Disaster Recovery)
resource "aws_s3_bucket" "datalake_dr" {
  provider      = aws.dr_region
  bucket        = "hyperflix-datalake-dr-${random_id.bucket_suffix.hex}"
  force_destroy = false

  tags = {
    Project     = "HYPERFLIX"
    Environment = var.environment
    Layer       = "DataLake-DisasterRecovery"
    Region      = var.aws_dr_region
  }
}

resource "aws_s3_bucket_versioning" "dr_versioning" {
  provider = aws.dr_region
  bucket   = aws_s3_bucket.datalake_dr.id
  versioning_configuration {
    status = "Enabled"
  }
}

# Rol de IAM para permitir la replicación automática S3 Cross-Region
resource "aws_iam_role" "replication_role" {
  name = "hyperflix-s3-replication-role-${random_id.bucket_suffix.hex}"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "s3.amazonaws.com"
        }
      }
    ]
  })
}

resource "aws_iam_policy" "replication_policy" {
  name = "hyperflix-s3-replication-policy-${random_id.bucket_suffix.hex}"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = [
          "s3:GetReplicationConfiguration",
          "s3:ListBucket"
        ]
        Effect   = "Allow"
        Resource = [aws_s3_bucket.datalake_primary.arn]
      },
      {
        Action = [
          "s3:GetObjectVersionForReplication",
          "s3:GetObjectVersionAcl",
          "s3:GetObjectVersionTagging"
        ]
        Effect   = "Allow"
        Resource = ["${aws_s3_bucket.datalake_primary.arn}/*"]
      },
      {
        Action = [
          "s3:ReplicateObject",
          "s3:ReplicateDelete",
          "s3:ReplicateTags"
        ]
        Effect   = "Allow"
        Resource = ["${aws_s3_bucket.datalake_dr.arn}/*"]
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "replication_attach" {
  role       = aws_iam_role.replication_role.name
  policy_arn = aws_iam_policy.replication_policy.arn
}

# Configuración de S3 Cross-Region Replication (CRR)
resource "aws_s3_bucket_replication_configuration" "crr_config" {
  depends_on = [aws_s3_bucket_versioning.primary_versioning]
  role       = aws_iam_role.replication_role.arn
  bucket     = aws_s3_bucket.datalake_primary.id

  rule {
    id     = "ReplicateAllDataLakePartitionsToDR"
    status = "Enabled"

    destination {
      bucket        = aws_s3_bucket.datalake_dr.arn
      storage_class = "STANDARD_IA" # Optimización de costos en región pasiva
    }
  }
}
