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

  # API Management settings
  publisher_name  = "Lotus Retail"
  publisher_email = "admin@lotusretail.example"
}