output "namespace_id" {
  description = "The ID of the Event Hubs namespace"
  value       = azurerm_eventhub_namespace.eventhub.id
}

output "namespace_name" {
  description = "The name of the Event Hubs namespace"
  value       = azurerm_eventhub_namespace.eventhub.name
}

output "price_changes_id" {
  description = "The ID of the price-changes Event Hub"
  value       = azurerm_eventhub.price_changes.id
}

output "promotion_events_id" {
  description = "The ID of the promotion-events Event Hub"
  value       = azurerm_eventhub.promotion_events.id
}

output "market_trends_id" {
  description = "The ID of the market-trends Event Hub"
  value       = azurerm_eventhub.market_trends.id
}

output "alerts_id" {
  description = "The ID of the alerts Event Hub"
  value       = azurerm_eventhub.alerts.id
}

output "connection_string" {
  description = "The connection string for the Event Hubs namespace"
  value       = azurerm_eventhub_namespace.eventhub.default_primary_connection_string
  sensitive   = true
}

output "sender_connection_string" {
  description = "The connection string for sending data to Event Hubs"
  value       = azurerm_eventhub_namespace_authorization_rule.sender.primary_connection_string
  sensitive   = true
}

output "processor_connection_string" {
  description = "The connection string for processing data from Event Hubs"
  value       = azurerm_eventhub_namespace_authorization_rule.processor.primary_connection_string
  sensitive   = true
} 