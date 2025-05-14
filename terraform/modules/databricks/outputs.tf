output "workspace_id" {
  description = "The ID of the Databricks workspace"
  value       = azurerm_databricks_workspace.databricks.id
}

output "workspace_url" {
  description = "The URL of the Databricks workspace"
  value       = azurerm_databricks_workspace.databricks.workspace_url
}

output "workspace_name" {
  description = "The name of the Databricks workspace"
  value       = azurerm_databricks_workspace.databricks.name
}

output "managed_resource_group_id" {
  description = "The ID of the managed resource group for the Databricks workspace"
  value       = azurerm_databricks_workspace.databricks.managed_resource_group_id
} 