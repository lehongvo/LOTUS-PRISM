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
# synapse_sql_admin_password = "P@ssw0rd1234!" # Use a more secure way to manage secrets in production

# Databricks
databricks_sku = "standard"

# Event Hubs
eventhub_sku      = "Standard"
eventhub_capacity = 1

# API Management
api_management_sku      = "Developer"
api_management_capacity = 1
publisher_name          = "Lotus Retail"
publisher_email         = "admin@lotusretail.example"