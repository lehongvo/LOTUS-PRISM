locals {
  resource_group_name   = var.resource_group_name != "" ? var.resource_group_name : "${var.project_name}-${var.environment}-rg"
  storage_account_name  = var.storage_account_name != "" ? var.storage_account_name : "${replace(var.project_name, "-", "")}${var.environment}adls"
  databricks_name       = "${var.project_name}-${var.environment}-databricks"
  synapse_name          = "${var.project_name}-${var.environment}-synapse"
  eventhub_name         = "${var.project_name}-${var.environment}-eventhub"
  api_management_name   = "${var.project_name}-${var.environment}-apim"
}

# Resource Group
resource "azurerm_resource_group" "rg" {
  name     = local.resource_group_name
  location = var.location
  tags     = var.tags
}

# Storage Account (Data Lake)
module "storage" {
  source = "./modules/storage"

  resource_group_name              = azurerm_resource_group.rg.name
  location                         = var.location
  storage_account_name             = local.storage_account_name
  storage_account_tier             = var.storage_account_tier
  storage_account_replication_type = var.storage_account_replication_type
  tags                             = var.tags
}

# Databricks
module "databricks" {
  source = "./modules/databricks"

  resource_group_name = azurerm_resource_group.rg.name
  location            = var.location
  databricks_name     = local.databricks_name
  databricks_sku      = var.databricks_sku
  tags                = var.tags
}

# Synapse Analytics
module "synapse" {
  source = "./modules/synapse"

  resource_group_name        = azurerm_resource_group.rg.name
  location                   = var.location
  synapse_name               = local.synapse_name
  storage_account_id         = module.storage.storage_account_id
  sql_admin_username         = var.synapse_sql_admin_username
  sql_admin_password         = var.synapse_sql_admin_password
  tags                       = var.tags
}

# Event Hubs
module "eventhubs" {
  source = "./modules/eventhubs"

  resource_group_name  = azurerm_resource_group.rg.name
  location             = var.location
  eventhub_name        = local.eventhub_name
  eventhub_sku         = var.eventhub_sku
  eventhub_capacity    = var.eventhub_capacity
  tags                 = var.tags
}

# API Management
module "api" {
  source = "./modules/api"

  resource_group_name       = azurerm_resource_group.rg.name
  location                  = var.location
  api_management_name       = local.api_management_name
  api_management_sku        = var.api_management_sku
  api_management_capacity   = var.api_management_capacity
  publisher_name            = var.publisher_name
  publisher_email           = var.publisher_email
  tags                      = var.tags
} 