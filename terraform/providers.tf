terraform {
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 3.0"
    }
    azuread = {
      source  = "hashicorp/azuread"
      version = "~> 2.0"
    }
  }
  backend "azurerm" {
    # These values must be configured through backend-config
    # resource_group_name  = "terraform-state-rg"
    # storage_account_name = "lotusterraformstate"
    # container_name       = "tfstate"
    # key                  = "lotus-prism.tfstate"
  }
  required_version = ">= 1.0.0"
}

provider "azurerm" {
  features {
    key_vault {
      purge_soft_delete_on_destroy = true
    }
  }
}

provider "azuread" {
  # Configuration options
} 