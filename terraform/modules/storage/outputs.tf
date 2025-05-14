output "storage_account_id" {
  description = "The ID of the storage account"
  value       = azurerm_storage_account.storage.id
}

output "storage_account_name" {
  description = "The name of the storage account"
  value       = azurerm_storage_account.storage.name
}

output "primary_access_key" {
  description = "The primary access key for the storage account"
  value       = azurerm_storage_account.storage.primary_access_key
  sensitive   = true
}

output "primary_blob_endpoint" {
  description = "The primary blob endpoint URL"
  value       = azurerm_storage_account.storage.primary_blob_endpoint
}

output "primary_dfs_endpoint" {
  description = "The primary Data Lake Storage Gen2 endpoint"
  value       = azurerm_storage_account.storage.primary_dfs_endpoint
}

output "filesystem_bronze_name" {
  description = "The name of the bronze filesystem"
  value       = azurerm_storage_data_lake_gen2_filesystem.bronze.name
}

output "filesystem_silver_name" {
  description = "The name of the silver filesystem"
  value       = azurerm_storage_data_lake_gen2_filesystem.silver.name
}

output "filesystem_gold_name" {
  description = "The name of the gold filesystem"
  value       = azurerm_storage_data_lake_gen2_filesystem.gold.name
} 