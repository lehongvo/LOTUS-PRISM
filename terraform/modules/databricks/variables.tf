variable "resource_group_name" {
  description = "The name of the resource group"
  type        = string
}

variable "location" {
  description = "The Azure region to deploy resources"
  type        = string
}

variable "databricks_name" {
  description = "The name of the Databricks workspace"
  type        = string
}

variable "databricks_sku" {
  description = "The SKU of the Databricks workspace (standard, premium, trial)"
  type        = string
  default     = "standard"

  validation {
    condition     = contains(["standard", "premium", "trial"], var.databricks_sku)
    error_message = "The databricks_sku must be one of: standard, premium, trial."
  }
}

variable "tags" {
  description = "Tags to apply to the Databricks workspace"
  type        = map(string)
  default     = {}
}

# Variables for VNet integration (commented out as they are only used in the advanced configuration)
# variable "vnet_id" {
#   description = "The ID of the virtual network for Databricks"
#   type        = string
#   default     = null
# }
#
# variable "public_subnet_name" {
#   description = "The name of the public subnet for Databricks"
#   type        = string
#   default     = null
# }
#
# variable "private_subnet_name" {
#   description = "The name of the private subnet for Databricks"
#   type        = string
#   default     = null
# } 