output "workspace_id" {
  description = "The ID of the Synapse workspace"
  value       = azurerm_synapse_workspace.synapse.id
}

output "workspace_name" {
  description = "The name of the Synapse workspace"
  value       = azurerm_synapse_workspace.synapse.name
}

output "sql_endpoint" {
  description = "The SQL endpoint of the Synapse workspace"
  value       = azurerm_synapse_workspace.synapse.connectivity_endpoints["sql"]
}

output "dev_endpoint" {
  description = "The dev endpoint of the Synapse workspace"
  value       = azurerm_synapse_workspace.synapse.connectivity_endpoints["dev"]
}

output "sql_pool_id" {
  description = "The ID of the Synapse SQL Pool"
  value       = var.create_sql_pool ? azurerm_synapse_sql_pool.sql_pool[0].id : null
}

output "spark_pool_id" {
  description = "The ID of the Synapse Spark Pool"
  value       = var.create_spark_pool ? azurerm_synapse_spark_pool.spark_pool[0].id : null
}

output "identity_principal_id" {
  description = "The principal ID of the system-assigned identity of the Synapse workspace"
  value       = azurerm_synapse_workspace.synapse.identity[0].principal_id
} 