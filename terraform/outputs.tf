# =============================================================================
# HYPERFLIX - OUTPUTS DE INFRAESTRUCTURA (IaC)
# =============================================================================

output "project_id" {
  description = "ID del proyecto aprovisionado en MongoDB Atlas"
  value       = mongodbatlas_project.hyperflix_project.id
}

output "cluster_name" {
  description = "Nombre del clúster de base de datos"
  value       = mongodbatlas_advanced_cluster.hyperflix_cluster.name
}

output "connection_string_standard" {
  description = "Cadena de conexión estándar (SRV) a MongoDB Atlas"
  value       = mongodbatlas_advanced_cluster.hyperflix_cluster.connection_strings[0].standard_srv
}

output "database_user" {
  description = "Usuario de base de datos configurado"
  value       = mongodbatlas_database_user.db_user.username
}

output "s3_primary_bucket_name" {
  description = "Nombre del bucket S3 primario del Data Lake (us-east-1)"
  value       = aws_s3_bucket.datalake_primary.id
}

output "s3_primary_bucket_arn" {
  description = "ARN del bucket primario"
  value       = aws_s3_bucket.datalake_primary.arn
}

output "s3_dr_bucket_name" {
  description = "Nombre del bucket S3 secundario de Disaster Recovery (us-west-2)"
  value       = aws_s3_bucket.datalake_dr.id
}

output "s3_dr_bucket_arn" {
  description = "ARN del bucket de Disaster Recovery"
  value       = aws_s3_bucket.datalake_dr.arn
}

output "replication_status" {
  description = "Estado de la configuración de Cross-Region Replication (CRR)"
  value       = "ACTIVADA - Replicación continua hacia AWS us-west-2"
}
