variable "resource_group_name" {
  description = "The name of the resource group"
  type        = string
}

variable "location" {
  description = "The Azure region to deploy resources"
  type        = string
}

variable "synapse_name" {
  description = "The name of the Synapse workspace"
  type        = string
}

variable "storage_account_id" {
  description = "The ID of the storage account for Synapse"
  type        = string
}

variable "sql_admin_username" {
  description = "SQL admin username for Synapse"
  type        = string
  default     = "sqladmin"
}

variable "sql_admin_password" {
  description = "SQL admin password for Synapse"
  type        = string
  sensitive   = true
}

variable "allowed_ip_address" {
  description = "Specific IP address to allow access to Synapse"
  type        = string
  default     = null
}

variable "create_sql_pool" {
  description = "Whether to create a Synapse SQL Pool"
  type        = bool
  default     = false
}

variable "sql_pool_sku" {
  description = "The SKU for the Synapse SQL Pool"
  type        = string
  default     = "DW100c"
}

variable "create_spark_pool" {
  description = "Whether to create a Synapse Spark Pool"
  type        = bool
  default     = false
}

variable "spark_node_size_family" {
  description = "The node size family for the Spark Pool"
  type        = string
  default     = "MemoryOptimized"
}

variable "spark_node_size" {
  description = "The node size for the Spark Pool"
  type        = string
  default     = "Small"
}

variable "spark_min_node_count" {
  description = "The minimum number of nodes for the Spark Pool"
  type        = number
  default     = 3
}

variable "spark_max_node_count" {
  description = "The maximum number of nodes for the Spark Pool"
  type        = number
  default     = 10
}

variable "spark_auto_pause_delay_in_minutes" {
  description = "The auto-pause delay in minutes for the Spark Pool"
  type        = number
  default     = 15
}

variable "spark_library_requirement" {
  description = "The library requirements for the Spark Pool"
  type        = string
  default     = null
}

variable "tags" {
  description = "Tags to apply to the Synapse workspace"
  type        = map(string)
  default     = {}
} 