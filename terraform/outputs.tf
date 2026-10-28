# =============================================================================
# HYPERFLIX - OUTPUTS DE INFRAESTRUCTURA COMO CÓDIGO (IaC)
# =============================================================================
# Refleja el clúster real gratuito M0 en MongoDB Atlas del estudiante
# =============================================================================

output "cluster_connection_string" {
  description = "Cadena de conexión estándar (SRV) a MongoDB Atlas"
  value       = mongodbatlas_advanced_cluster.hyperflix_cluster.connection_strings[0].standard_srv
  sensitive   = true
}

output "cluster_id" {
  description = "Identificador único del clúster en MongoDB Atlas"
  value       = mongodbatlas_advanced_cluster.hyperflix_cluster.cluster_id
}

output "cluster_info" {
  description = "Ficha técnica y especificaciones del clúster de base de datos"
  value = {
    backup      = false
    disk_gb     = 0.5
    instance    = var.cluster_tier
    name        = var.cluster_name
    provider    = var.cloud_provider
    region      = var.primary_region
    replication = "3 nodos (REPLICASET)"
  }
}

output "cluster_state" {
  description = "Estado de aprovisionamiento del clúster en la nube"
  value       = mongodbatlas_advanced_cluster.hyperflix_cluster.state_name
}

output "database_username" {
  description = "Usuario administrador de base de datos aprovisionado"
  value       = mongodbatlas_database_user.db_user.username
}

output "project_id" {
  description = "ID del proyecto en MongoDB Atlas"
  value       = mongodbatlas_project.hyperflix_project.id
}
