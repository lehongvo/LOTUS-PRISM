resource "azurerm_databricks_workspace" "databricks" {
  name                = var.databricks_name
  resource_group_name = var.resource_group_name
  location            = var.location
  sku                 = var.databricks_sku

  tags = var.tags
}

# The following could be used if more advanced configuration is needed
# For example, custom virtual network integration
# resource "azurerm_databricks_workspace" "databricks" {
#   name                        = var.databricks_name
#   resource_group_name         = var.resource_group_name
#   location                    = var.location
#   sku                         = var.databricks_sku
#   managed_resource_group_name = "${var.databricks_name}-managed-rg"
#
#   custom_parameters {
#     no_public_ip        = true
#     virtual_network_id  = var.vnet_id
#     public_subnet_name  = var.public_subnet_name
#     private_subnet_name = var.private_subnet_name
#   }
#
#   tags = var.tags
# } 