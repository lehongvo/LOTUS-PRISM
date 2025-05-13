resource "azurerm_synapse_workspace" "synapse" {
  name                                 = var.synapse_name
  resource_group_name                  = var.resource_group_name
  location                             = var.location
  storage_data_lake_gen2_filesystem_id = "${var.storage_account_id}/dfs/synapse"
  sql_administrator_login              = var.sql_admin_username
  sql_administrator_login_password     = var.sql_admin_password

  identity {
    type = "SystemAssigned"
  }

  tags = var.tags
}

# Synapse SQL Pool (formerly SQL DW) - Optional, as it incurs costs
resource "azurerm_synapse_sql_pool" "sql_pool" {
  count                = var.create_sql_pool ? 1 : 0
  name                 = "${var.synapse_name}-sqlpool"
  synapse_workspace_id = azurerm_synapse_workspace.synapse.id
  sku_name             = var.sql_pool_sku
  create_mode          = "Default"
  
  data_encrypted       = true  # Enable encryption at rest
  
  tags                 = var.tags
}

# Apache Spark Pool - Optional, as it incurs costs
resource "azurerm_synapse_spark_pool" "spark_pool" {
  count                = var.create_spark_pool ? 1 : 0
  name                 = "${replace(var.synapse_name, "-", "")}sparkpool"
  synapse_workspace_id = azurerm_synapse_workspace.synapse.id
  node_size_family     = var.spark_node_size_family
  node_size            = var.spark_node_size
  
  auto_scale {
    max_node_count = var.spark_max_node_count
    min_node_count = var.spark_min_node_count
  }
  
  auto_pause {
    delay_in_minutes = var.spark_auto_pause_delay_in_minutes
  }
  
  dynamic "library_requirement" {
    for_each = var.spark_library_requirement != null ? [var.spark_library_requirement] : []
    content {
      content  = library_requirement.value
      filename = "requirements.txt"
    }
  }
  
  tags = var.tags
}

# Firewall rule to allow Azure services
resource "azurerm_synapse_firewall_rule" "allow_azure_services" {
  name                 = "AllowAllWindowsAzureIps"
  synapse_workspace_id = azurerm_synapse_workspace.synapse.id
  start_ip_address     = "0.0.0.0"
  end_ip_address       = "0.0.0.0"
}

# Firewall rule to allow specific IP if specified
resource "azurerm_synapse_firewall_rule" "allow_specific_ip" {
  count                = var.allowed_ip_address != null ? 1 : 0
  name                 = "AllowSpecificIP"
  synapse_workspace_id = azurerm_synapse_workspace.synapse.id
  start_ip_address     = var.allowed_ip_address
  end_ip_address       = var.allowed_ip_address
} 