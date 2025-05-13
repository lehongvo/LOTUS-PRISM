output "resource_group_name" {
  description = "The name of the resource group"
  value       = module.lotus_prism.resource_group_name
}

output "storage_account_name" {
  description = "The name of the storage account"
  value       = module.lotus_prism.storage_account_name
}

output "adls_filesystem_bronze_name" {
  description = "The name of the bronze filesystem in ADLS"
  value       = module.lotus_prism.adls_filesystem_bronze_name
}

output "adls_filesystem_silver_name" {
  description = "The name of the silver filesystem in ADLS"
  value       = module.lotus_prism.adls_filesystem_silver_name
}

output "adls_filesystem_gold_name" {
  description = "The name of the gold filesystem in ADLS"
  value       = module.lotus_prism.adls_filesystem_gold_name
}

output "databricks_workspace_url" {
  description = "The URL of the Databricks workspace"
  value       = module.lotus_prism.databricks_workspace_url
}

output "synapse_workspace_id" {
  description = "The ID of the Synapse workspace"
  value       = module.lotus_prism.synapse_workspace_id
}

output "synapse_sql_endpoint" {
  description = "The SQL endpoint of the Synapse workspace"
  value       = module.lotus_prism.synapse_sql_endpoint
}

output "eventhub_namespace_name" {
  description = "The name of the Event Hub namespace"
  value       = module.lotus_prism.eventhub_namespace_name
}

output "api_management_name" {
  description = "The name of the API Management service"
  value       = module.lotus_prism.api_management_name
}

output "api_management_gateway_url" {
  description = "The gateway URL of the API Management service"
  value       = module.lotus_prism.api_management_gateway_url
} 