resource "azurerm_eventhub_namespace" "eventhub" {
  name                = var.eventhub_name
  location            = var.location
  resource_group_name = var.resource_group_name
  sku                 = var.eventhub_sku
  capacity            = var.eventhub_capacity
  
  auto_inflate_enabled     = var.auto_inflate_enabled
  maximum_throughput_units = var.maximum_throughput_units
  
  tags = var.tags
}

# Event Hub for price change events
resource "azurerm_eventhub" "price_changes" {
  name                = "price-changes"
  namespace_name      = azurerm_eventhub_namespace.eventhub.name
  resource_group_name = var.resource_group_name
  partition_count     = var.partition_count
  message_retention   = var.message_retention
}

# Event Hub for promotion events
resource "azurerm_eventhub" "promotion_events" {
  name                = "promotion-events"
  namespace_name      = azurerm_eventhub_namespace.eventhub.name
  resource_group_name = var.resource_group_name
  partition_count     = var.partition_count
  message_retention   = var.message_retention
}

# Event Hub for market trend events
resource "azurerm_eventhub" "market_trends" {
  name                = "market-trends"
  namespace_name      = azurerm_eventhub_namespace.eventhub.name
  resource_group_name = var.resource_group_name
  partition_count     = var.partition_count
  message_retention   = var.message_retention
}

# Event Hub for alerts and notifications
resource "azurerm_eventhub" "alerts" {
  name                = "alerts"
  namespace_name      = azurerm_eventhub_namespace.eventhub.name
  resource_group_name = var.resource_group_name
  partition_count     = var.partition_count
  message_retention   = var.message_retention
}

# Consumer Group for Databricks
resource "azurerm_eventhub_consumer_group" "databricks" {
  for_each            = toset(["price-changes", "promotion-events", "market-trends", "alerts"])
  name                = "databricks"
  eventhub_name       = each.key == "price-changes" ? azurerm_eventhub.price_changes.name : each.key == "promotion-events" ? azurerm_eventhub.promotion_events.name : each.key == "market-trends" ? azurerm_eventhub.market_trends.name : azurerm_eventhub.alerts.name
  namespace_name      = azurerm_eventhub_namespace.eventhub.name
  resource_group_name = var.resource_group_name
}

# Consumer Group for Stream Analytics
resource "azurerm_eventhub_consumer_group" "stream_analytics" {
  for_each            = toset(["price-changes", "promotion-events", "market-trends", "alerts"])
  name                = "stream-analytics"
  eventhub_name       = each.key == "price-changes" ? azurerm_eventhub.price_changes.name : each.key == "promotion-events" ? azurerm_eventhub.promotion_events.name : each.key == "market-trends" ? azurerm_eventhub.market_trends.name : azurerm_eventhub.alerts.name
  namespace_name      = azurerm_eventhub_namespace.eventhub.name
  resource_group_name = var.resource_group_name
}

# Authorization Rule for sending data
resource "azurerm_eventhub_namespace_authorization_rule" "sender" {
  name                = "sender"
  namespace_name      = azurerm_eventhub_namespace.eventhub.name
  resource_group_name = var.resource_group_name
  
  listen = false
  send   = true
  manage = false
}

# Authorization Rule for processing data
resource "azurerm_eventhub_namespace_authorization_rule" "processor" {
  name                = "processor"
  namespace_name      = azurerm_eventhub_namespace.eventhub.name
  resource_group_name = var.resource_group_name
  
  listen = true
  send   = true
  manage = false
} 