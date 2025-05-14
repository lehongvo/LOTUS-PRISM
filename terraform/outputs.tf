output "resource_group_name" {
  description = "The name of the resource group"
  value       = azurerm_resource_group.rg.name
}

output "resource_group_id" {
  description = "The ID of the resource group"
  value       = azurerm_resource_group.rg.id
}

# Storage
output "storage_account_name" {
  description = "The name of the storage account"
  value       = module.storage.storage_account_name
}

output "storage_account_id" {
  description = "The ID of the storage account"
  value       = module.storage.storage_account_id
}

output "adls_filesystem_bronze_name" {
  description = "The name of the bronze filesystem in ADLS"
  value       = module.storage.filesystem_bronze_name
}

output "adls_filesystem_silver_name" {
  description = "The name of the silver filesystem in ADLS"
  value       = module.storage.filesystem_silver_name
}

output "adls_filesystem_gold_name" {
  description = "The name of the gold filesystem in ADLS"
  value       = module.storage.filesystem_gold_name
}

# Databricks
output "databricks_workspace_url" {
  description = "The URL of the Databricks workspace"
  value       = module.databricks.workspace_url
}

output "databricks_workspace_id" {
  description = "The ID of the Databricks workspace"
  value       = module.databricks.workspace_id
}

# Synapse
output "synapse_workspace_id" {
  description = "The ID of the Synapse workspace"
  value       = module.synapse.workspace_id
}

output "synapse_sql_endpoint" {
  description = "The SQL endpoint of the Synapse workspace"
  value       = module.synapse.sql_endpoint
}

# Event Hubs
output "eventhub_namespace_name" {
  description = "The name of the Event Hub namespace"
  value       = module.eventhubs.namespace_name
}

output "eventhub_connection_string" {
  description = "The connection string for the Event Hub namespace"
  value       = module.eventhubs.connection_string
  sensitive   = true
}

# API Management
output "api_management_name" {
  description = "The name of the API Management service"
  value       = module.api.api_management_name
}

output "api_management_gateway_url" {
  description = "The gateway URL of the API Management service"
  value       = module.api.gateway_url
} 