variable "resource_group_name" {
  description = "The name of the resource group"
  type        = string
}

variable "location" {
  description = "The Azure region to deploy resources"
  type        = string
}

variable "eventhub_name" {
  description = "The name of the Event Hubs namespace"
  type        = string
}

variable "eventhub_sku" {
  description = "The SKU of the Event Hubs namespace"
  type        = string
  default     = "Standard"
}

variable "eventhub_capacity" {
  description = "The throughput units for the Event Hubs namespace"
  type        = number
  default     = 1
}

variable "auto_inflate_enabled" {
  description = "Whether auto-inflate is enabled for the Event Hubs namespace"
  type        = bool
  default     = true
}

variable "maximum_throughput_units" {
  description = "The maximum throughput units for the Event Hubs namespace when auto-inflate is enabled"
  type        = number
  default     = 10
}

variable "partition_count" {
  description = "The number of partitions for the Event Hubs"
  type        = number
  default     = 4
}

variable "message_retention" {
  description = "The retention period for messages in days"
  type        = number
  default     = 1
}

variable "tags" {
  description = "Tags to apply to the Event Hubs namespace"
  type        = map(string)
  default     = {}
} 