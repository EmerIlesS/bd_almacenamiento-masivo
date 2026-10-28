# =============================================================================
# HYPERFLIX - INFRAESTRUCTURA COMO CÓDIGO (IaC) - MONGODB ATLAS
# =============================================================================
# Arquitectura Real: Clúster Gratuito M0 en MongoDB Atlas (AWS US_EAST_1)
# =============================================================================

provider "mongodbatlas" {
  public_key  = var.atlas_public_key
  private_key = var.atlas_private_key
}

# -----------------------------------------------------------------------------
# 1. MONGODB ATLAS: PROYECTO & CLÚSTER GRATUITO (M0 SANDBOX)
# -----------------------------------------------------------------------------

resource "mongodbatlas_project" "hyperflix_project" {
  name   = var.project_name
  org_id = var.atlas_org_id
}

# Clúster Gratuito M0 en la nube de MongoDB Atlas
resource "mongodbatlas_advanced_cluster" "hyperflix_cluster" {
  project_id   = mongodbatlas_project.hyperflix_project.id
  name         = var.cluster_name
  cluster_type = "REPLICASET"

  replication_specs {
    num_shards = 1

    region_configs {
      electable_specs {
        instance_size = var.cluster_tier
      }
      provider_name         = "TENANT"
      backing_provider_name = var.cloud_provider
      region_name           = var.primary_region
      priority              = 7
    }
  }

  # Los respaldos automáticos no aplican en la capa gratuita M0
  backup_enabled = false
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
