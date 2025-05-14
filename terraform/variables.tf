variable "project_name" {
  description = "The name of the project"
  type        = string
  default     = "lotus-prism"
}

variable "environment" {
  description = "Environment name (dev/prod)"
  type        = string
  default     = "dev"
}

variable "location" {
  description = "Azure region for resources"
  type        = string
  default     = "eastasia"
}

variable "tags" {
  description = "Tags to apply to all resources"
  type        = map(string)
  default = {
    "Project"     = "LOTUS-PRISM"
    "Environment" = "Development"
    "Owner"       = "Data Engineering Team"
    "Creator"     = "Terraform"
  }
}

# Resource Group
variable "resource_group_name" {
  description = "Name of the resource group"
  type        = string
  default     = ""
}

# Storage
variable "storage_account_name" {
  description = "Name of the storage account for data lake"
  type        = string
  default     = ""
}

variable "storage_account_tier" {
  description = "Tier of the storage account"
  type        = string
  default     = "Standard"
}

variable "storage_account_replication_type" {
  description = "Replication type of the storage account"
  type        = string
  default     = "LRS"
}

# Databricks
variable "databricks_sku" {
  description = "SKU for Databricks workspace"
  type        = string
  default     = "standard"
}

# Synapse
variable "synapse_sql_admin_username" {
  description = "SQL administrator username for Synapse workspace"
  type        = string
  default     = "sqladmin"
}

variable "synapse_sql_admin_password" {
  description = "SQL administrator password for Synapse workspace"
  type        = string
  sensitive   = true
}

# Event Hubs
variable "eventhub_sku" {
  description = "SKU for Event Hub Namespace"
  type        = string
  default     = "Standard"
}

variable "eventhub_capacity" {
  description = "Throughput units for Event Hub Namespace"
  type        = number
  default     = 1
}

# API Management
variable "api_management_sku" {
  description = "SKU for API Management"
  type        = string
  default     = "Developer"
}

variable "api_management_capacity" {
  description = "Capacity units for API Management"
  type        = number
  default     = 1
}

variable "publisher_name" {
  description = "Publisher name for API Management"
  type        = string
  default     = "Lotus Retail"
}

variable "publisher_email" {
  description = "Email address for API Management publisher"
  type        = string
} 