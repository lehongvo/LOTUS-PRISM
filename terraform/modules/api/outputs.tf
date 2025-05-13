output "api_management_id" {
  description = "The ID of the API Management service"
  value       = azurerm_api_management.apim.id
}

output "api_management_name" {
  description = "The name of the API Management service"
  value       = azurerm_api_management.apim.name
}

output "gateway_url" {
  description = "The gateway URL of the API Management service"
  value       = azurerm_api_management.apim.gateway_url
}

output "management_api_url" {
  description = "The management API URL of the API Management service"
  value       = azurerm_api_management.apim.management_api_url
}

output "portal_url" {
  description = "The portal URL of the API Management service"
  value       = azurerm_api_management.apim.portal_url
}

output "developer_portal_url" {
  description = "The developer portal URL of the API Management service"
  value       = azurerm_api_management.apim.developer_portal_url
}

output "identity_principal_id" {
  description = "The principal ID of the system-assigned identity of the API Management service"
  value       = azurerm_api_management.apim.identity[0].principal_id
}

output "analytics_subscription_key" {
  description = "The primary key for the analytics subscription"
  value       = azurerm_api_management_subscription.subscription.primary_key
  sensitive   = true
} 