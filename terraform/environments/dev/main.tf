terraform {
  backend "azurerm" {
    # These values must be configured through backend-config during terraform init
    # resource_group_name  = "terraform-state-rg"
    # storage_account_name = "lotusterraformstate"
    # container_name       = "tfstate"
    # key                  = "lotus-prism-dev.tfstate"
  }
}

module "lotus_prism" {
  source = "../../"

  # Project settings
  project_name = "lotus-prism"
  environment  = "dev"
  location     = "East Asia"
  tags = {
    Project     = "LOTUS-PRISM"
    Environment = "Development"
    Creator     = "Terraform"
    Owner       = "Data Engineering Team"
  }

  # SQL credentials for Synapse
  synapse_sql_admin_username = "sqladmin"
  synapse_sql_admin_password = "P@ssw0rd1234!" # In production, use a more secure way to manage secrets

  # Event Hubs settings
  eventhub_sku      = "Standard"
  eventhub_capacity = 1

  # API Management settings
  api_management_sku      = "Developer"
  api_management_capacity = 1
  publisher_name          = "Lotus Retail"
  publisher_email         = "admin@lotusretail.example"
}