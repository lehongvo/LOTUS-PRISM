resource "azurerm_storage_account" "storage" {
  name                     = var.storage_account_name
  resource_group_name      = var.resource_group_name
  location                 = var.location
  account_tier             = var.storage_account_tier
  account_replication_type = var.storage_account_replication_type
  account_kind             = "StorageV2"
  is_hns_enabled           = true // Enable hierarchical namespace for ADLS Gen2
  
  blob_properties {
    versioning_enabled = false
    delete_retention_policy {
      days = 7
    }
  }

  tags = var.tags
}

# Bronze Layer (Raw data)
resource "azurerm_storage_data_lake_gen2_filesystem" "bronze" {
  name               = "bronze"
  storage_account_id = azurerm_storage_account.storage.id
}

# Silver Layer (Cleaned and normalized data)
resource "azurerm_storage_data_lake_gen2_filesystem" "silver" {
  name               = "silver"
  storage_account_id = azurerm_storage_account.storage.id
}

# Gold Layer (Business-ready data)
resource "azurerm_storage_data_lake_gen2_filesystem" "gold" {
  name               = "gold"
  storage_account_id = azurerm_storage_account.storage.id
}

# Create folders within each layer
resource "azurerm_storage_data_lake_gen2_path" "bronze_competitors" {
  path               = "competitors"
  filesystem_name    = azurerm_storage_data_lake_gen2_filesystem.bronze.name
  storage_account_id = azurerm_storage_account.storage.id
  resource           = "directory"
}

resource "azurerm_storage_data_lake_gen2_path" "bronze_internal" {
  path               = "internal"
  filesystem_name    = azurerm_storage_data_lake_gen2_filesystem.bronze.name
  storage_account_id = azurerm_storage_account.storage.id
  resource           = "directory"
}

resource "azurerm_storage_data_lake_gen2_path" "bronze_market_trends" {
  path               = "market_trends"
  filesystem_name    = azurerm_storage_data_lake_gen2_filesystem.bronze.name
  storage_account_id = azurerm_storage_account.storage.id
  resource           = "directory"
}

resource "azurerm_storage_data_lake_gen2_path" "silver_normalized" {
  path               = "normalized"
  filesystem_name    = azurerm_storage_data_lake_gen2_filesystem.silver.name
  storage_account_id = azurerm_storage_account.storage.id
  resource           = "directory"
}

resource "azurerm_storage_data_lake_gen2_path" "silver_categorized" {
  path               = "categorized"
  filesystem_name    = azurerm_storage_data_lake_gen2_filesystem.silver.name
  storage_account_id = azurerm_storage_account.storage.id
  resource           = "directory"
}

resource "azurerm_storage_data_lake_gen2_path" "gold_price_analytics" {
  path               = "price_analytics"
  filesystem_name    = azurerm_storage_data_lake_gen2_filesystem.gold.name
  storage_account_id = azurerm_storage_account.storage.id
  resource           = "directory"
}

resource "azurerm_storage_data_lake_gen2_path" "gold_promotion_analytics" {
  path               = "promotion_analytics"
  filesystem_name    = azurerm_storage_data_lake_gen2_filesystem.gold.name
  storage_account_id = azurerm_storage_account.storage.id
  resource           = "directory"
}

resource "azurerm_storage_data_lake_gen2_path" "gold_product_analytics" {
  path               = "product_analytics"
  filesystem_name    = azurerm_storage_data_lake_gen2_filesystem.gold.name
  storage_account_id = azurerm_storage_account.storage.id
  resource           = "directory"
} 