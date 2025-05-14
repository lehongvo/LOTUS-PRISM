variable "resource_group_name" {
  description = "The name of the resource group"
  type        = string
}

variable "location" {
  description = "The Azure region to deploy resources"
  type        = string
}

variable "api_management_name" {
  description = "The name of the API Management service"
  type        = string
}

variable "api_management_sku" {
  description = "The SKU of the API Management service"
  type        = string
  default     = "Developer"
}

variable "api_management_capacity" {
  description = "The capacity of the API Management service"
  type        = number
  default     = 1
}

variable "publisher_name" {
  description = "The name of the publisher for the API Management service"
  type        = string
}

variable "publisher_email" {
  description = "The email address of the publisher for the API Management service"
  type        = string
}

variable "tags" {
  description = "Tags to apply to the API Management service"
  type        = map(string)
  default     = {}
} 