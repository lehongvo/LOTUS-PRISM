resource "azurerm_api_management" "apim" {
  name                = var.api_management_name
  location            = var.location
  resource_group_name = var.resource_group_name
  publisher_name      = var.publisher_name
  publisher_email     = var.publisher_email
  sku_name            = "${var.api_management_sku}_${var.api_management_capacity}"

  identity {
    type = "SystemAssigned"
  }

  tags = var.tags
}

# Create API
resource "azurerm_api_management_api" "price_analytics" {
  name                = "price-analytics"
  resource_group_name = var.resource_group_name
  api_management_name = azurerm_api_management.apim.name
  revision            = "1"
  display_name        = "Price Analytics API"
  path                = "price"
  protocols           = ["https"]
}

resource "azurerm_api_management_api" "promotion_analytics" {
  name                = "promotion-analytics"
  resource_group_name = var.resource_group_name
  api_management_name = azurerm_api_management.apim.name
  revision            = "1"
  display_name        = "Promotion Analytics API"
  path                = "promotion"
  protocols           = ["https"]
}

resource "azurerm_api_management_api" "product_analytics" {
  name                = "product-analytics"
  resource_group_name = var.resource_group_name
  api_management_name = azurerm_api_management.apim.name
  revision            = "1"
  display_name        = "Product Analytics API"
  path                = "product"
  protocols           = ["https"]
}

# Create Products (grouping of APIs)
resource "azurerm_api_management_product" "analytics" {
  product_id            = "analytics"
  api_management_name   = azurerm_api_management.apim.name
  resource_group_name   = var.resource_group_name
  display_name          = "Analytics APIs"
  subscription_required = true
  approval_required     = true
  published             = true
}

# Associate APIs with Products
resource "azurerm_api_management_product_api" "price_analytics" {
  api_name            = azurerm_api_management_api.price_analytics.name
  product_id          = azurerm_api_management_product.analytics.product_id
  api_management_name = azurerm_api_management.apim.name
  resource_group_name = var.resource_group_name
}

resource "azurerm_api_management_product_api" "promotion_analytics" {
  api_name            = azurerm_api_management_api.promotion_analytics.name
  product_id          = azurerm_api_management_product.analytics.product_id
  api_management_name = azurerm_api_management.apim.name
  resource_group_name = var.resource_group_name
}

resource "azurerm_api_management_product_api" "product_analytics" {
  api_name            = azurerm_api_management_api.product_analytics.name
  product_id          = azurerm_api_management_product.analytics.product_id
  api_management_name = azurerm_api_management.apim.name
  resource_group_name = var.resource_group_name
}

# Create a subscription for consuming APIs
resource "azurerm_api_management_subscription" "subscription" {
  name                = "lotus-subscription"
  api_management_name = azurerm_api_management.apim.name
  resource_group_name = var.resource_group_name
  product_id          = azurerm_api_management_product.analytics.id
  display_name        = "Lotus Analytics Subscription"
  state               = "active"
}

# Policy for rate limiting
resource "azurerm_api_management_product_policy" "rate_limit" {
  product_id          = azurerm_api_management_product.analytics.product_id
  api_management_name = azurerm_api_management.apim.name
  resource_group_name = var.resource_group_name

  xml_content = <<XML
<policies>
  <inbound>
    <rate-limit calls="300" renewal-period="60" />
    <quota calls="10000" renewal-period="86400" />
    <base />
  </inbound>
  <backend>
    <base />
  </backend>
  <outbound>
    <base />
  </outbound>
  <on-error>
    <base />
  </on-error>
</policies>
XML
} 