# =============================================================================
# HYPERFLIX - VARIABLES DE INFRAESTRUCTURA COMO CÓDIGO (IaC)
# =============================================================================

variable "atlas_public_key" {
  type        = string
  description = "Clave pública de la API de MongoDB Atlas"
  default     = ""
}

variable "atlas_private_key" {
  type        = string
  description = "Clave privada de la API de MongoDB Atlas"
  sensitive   = true
  default     = ""
}

variable "atlas_org_id" {
  type        = string
  description = "ID de Organización en MongoDB Atlas"
  default     = "org-hyperflix-prod"
}

variable "project_name" {
  type        = string
  description = "Nombre del proyecto en MongoDB Atlas"
  default     = "HYPERFLIX-Streaming-Platform"
}

variable "cluster_name" {
  type        = string
  description = "Nombre del Clúster de Base de Datos"
  default     = "hyperflix-cluster-prod"
}

variable "cluster_tier" {
  type        = string
  description = "Tier de cómputo del clúster (M10, M20, M30 para producción/DR)"
  default     = "M10"
}

variable "cloud_provider" {
  type        = string
  description = "Proveedor de nube principal"
  default     = "AWS"
}

variable "primary_region" {
  type        = string
  description = "Región principal de despliegue (AWS us-east-1)"
  default     = "US_EAST_1"
}

variable "dr_region" {
  type        = string
  description = "Región secundaria de Disaster Recovery (AWS us-west-2)"
  default     = "US_WEST_2"
}

variable "aws_primary_region" {
  type        = string
  description = "Nombre de región en formato AWS estándar para S3 primario"
  default     = "us-east-1"
}

variable "aws_dr_region" {
  type        = string
  description = "Nombre de región en formato AWS estándar para S3 DR"
  default     = "us-west-2"
}

variable "db_username" {
  type        = string
  description = "Usuario administrador de base de datos"
  default     = "hyperflix_admin"
}

variable "db_password" {
  type        = string
  description = "Contraseña para el usuario de base de datos"
  sensitive   = true
  default     = "HyperFlix2026SecurePass!"
}

variable "ip_whitelist" {
  type        = list(string)
  description = "Lista de direcciones IP o rangos CIDR autorizados para conectarse"
  default     = ["0.0.0.0/0"] # Configuración flexible para laboratorios y evaluación
}

variable "environment" {
  type        = string
  description = "Ambiente de despliegue"
  default     = "production"
}
