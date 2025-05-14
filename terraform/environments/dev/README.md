# LOTUS-PRISM Development Environment

This directory contains Terraform configuration for the development environment of the LOTUS-PRISM project.

## Resources Deployed

- Resource Group
- Azure Data Lake Storage Gen2 (with Bronze, Silver, Gold layers)
- Azure Databricks Workspace
- Azure Synapse Analytics
- Azure Event Hubs
- Azure API Management

## Deployment

### Prerequisites

1. Azure CLI installed and logged in
2. Terraform installed (version >= 1.0.0)
3. Access to Azure subscription with Contributor role

### Initialize Terraform

```bash
terraform init
```

### Create Azure Storage for Terraform State (Optional)

For team collaboration, it's recommended to use Azure Storage for Terraform state. 
Create a storage account and container, then update the backend configuration:

```bash
# Create resource group for Terraform state
az group create --name terraform-state-rg --location eastasia

# Create storage account
az storage account create --name lotusterraformstate --resource-group terraform-state-rg --sku Standard_LRS

# Create container
az storage container create --name tfstate --account-name lotusterraformstate

# Get storage account key
ACCOUNT_KEY=$(az storage account keys list --resource-group terraform-state-rg --account-name lotusterraformstate --query [0].value -o tsv)

# Initialize Terraform with Azure backend
terraform init -backend-config="resource_group_name=terraform-state-rg" \
               -backend-config="storage_account_name=lotusterraformstate" \
               -backend-config="container_name=tfstate" \
               -backend-config="key=lotus-prism-dev.tfstate" \
               -backend-config="access_key=$ACCOUNT_KEY"
```

### Plan and Apply

```bash
# See what will be created
terraform plan

# Apply the changes
terraform apply
```

### Secrets Management

For production use, avoid storing sensitive values like passwords in Terraform variables.
Instead, use Azure Key Vault or environment variables.

## Outputs

After deployment, you'll get outputs including:

- Azure resource URLs and endpoints
- Connection details for various services

## Clean Up

To destroy the environment when no longer needed:

```bash
terraform destroy
``` 