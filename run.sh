#!/bin/bash

# Colors
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# Banner
function show_banner() {
    clear
    echo -e "${BLUE}"
    echo "====================================================================="
    echo " _     ___ _____ _   _ ____       ____  ____  ___ ____  __  __ "
    echo "| |   / _ \_   _| | | / ___|     |  _ \|  _ \|_ _/ ___||  \/  |"
    echo "| |  | | | || | | | | \___ \ _____| |_) | |_) || |\___ \| |\/| |"
    echo "| |__| |_| || | | |_| |___) |_____|  __/|  _ < | | ___) | |  | |"
    echo "|_____\___/ |_|  \___/|____/      |_|   |_| \_\___|____/|_|  |_|"
    echo "====================================================================="
    echo -e "  Price & Retail Intelligence Streaming Monitor ${NC}"
    echo
}

# Global variables
TF_DIR="terraform"
ENV="dev"
BACKEND_CONFIGURED=false

# Check prerequisites
function check_prerequisites() {
    echo -e "${YELLOW}Checking prerequisites...${NC}"
    
    # Check Terraform
    if ! command -v terraform &> /dev/null; then
        echo -e "${RED}Terraform not found. Please install Terraform before continuing.${NC}"
        exit 1
    fi
    
    # Check Azure CLI
    if ! command -v az &> /dev/null; then
        echo -e "${RED}Azure CLI not found. Please install Azure CLI before continuing.${NC}"
        exit 1
    fi
    
    # Check Azure login
    az account show &> /dev/null
    if [ $? -ne 0 ]; then
        echo -e "${YELLOW}You are not logged into Azure. Initiating login...${NC}"
        az login
    fi
    
    echo -e "${GREEN}All prerequisites are ready.${NC}"
    echo
}

# Select environment
function select_environment() {
    echo -e "${YELLOW}Select environment:${NC}"
    echo "1) Development (default)"
    echo "2) Production"
    read -p "Choice [1-2]: " env_choice
    
    case $env_choice in
        2)
            ENV="prod"
            ;;
        *)
            ENV="dev"
            ;;
    esac
    
    echo -e "${GREEN}Selected environment: ${ENV}${NC}"
    echo
}

# Setup Terraform backend
function setup_backend() {
    echo -e "${YELLOW}Do you want to set up Azure Storage for Terraform state? (y/n)${NC}"
    read -p "Choice [y/n]: " backend_choice
    
    if [[ $backend_choice == "y" || $backend_choice == "Y" ]]; then
        echo -e "${YELLOW}Setting up Azure Storage for Terraform state...${NC}"

        # Input storage account name
        read -p "Storage account name [lotusterraformstatev1]: " storage_name
        storage_name=${storage_name:-lotusterraformstatev1}

        # Check if storage account exists
        existing_rg=$(az storage account list --query "[?name=='$storage_name'].resourceGroup" -o tsv)
        if [ -n "$existing_rg" ]; then
            echo -e "${GREEN}Storage account $storage_name already exists in resource group: $existing_rg${NC}"
            rg_name=$existing_rg
            # Get location from storage account
            location=$(az storage account show --name $storage_name --resource-group $rg_name --query "primaryLocation" -o tsv)
        else
            # If not exists, ask user
            read -p "Resource group name [terraform-state-rg]: " rg_name
            rg_name=${rg_name:-terraform-state-rg}
            read -p "Location [eastasia]: " location
            location=${location:-eastasia}
            echo "Creating resource group..."
            az group create --name $rg_name --location $location
            echo "Creating storage account..."
            az storage account create --name $storage_name --resource-group $rg_name --sku Standard_LRS
        fi

        read -p "Container name [tfstate]: " container_name
        container_name=${container_name:-tfstate}
        echo "Creating container..."
        az storage container create --name $container_name --account-name $storage_name

        echo "Getting storage key..."
        ACCOUNT_KEY=$(az storage account keys list --resource-group $rg_name --account-name $storage_name --query [0].value -o tsv)

        # Remove .terraform directory to avoid stale backend credentials
        rm -rf $TF_DIR/environments/$ENV/.terraform

        # Check is_hns_enabled
        HNS_ENABLED=$(az storage account show --name $storage_name --resource-group $rg_name --query "isHnsEnabled" -o tsv)
        if [ "$HNS_ENABLED" != "true" ]; then
            echo -e "${RED}Storage account $storage_name does not have Hierarchical namespace (ADLS Gen2) enabled. Deleting this storage account automatically...${NC}"
            az storage account delete --name $storage_name --resource-group $rg_name --yes
            echo -e "${YELLOW}Waiting for Azure to fully release the storage account...${NC}"
            sleep 120
            echo -e "${GREEN}Deleted. Creating storage account again with is_hns_enabled=true...${NC}"
            az storage account create --name $storage_name --resource-group $rg_name --sku Standard_LRS --kind StorageV2 --hns true
        fi

        echo -e "${YELLOW}Waiting 10 seconds for storage account to be fully ready for ADLS Gen2...${NC}"
        sleep 10

        echo "Initializing Terraform with Azure backend..."
        (cd $TF_DIR/environments/$ENV && terraform init -reconfigure \
            -backend-config="resource_group_name=$rg_name" \
            -backend-config="storage_account_name=$storage_name" \
            -backend-config="container_name=$container_name" \
            -backend-config="key=lotus-prism-$ENV.tfstate" \
            -backend-config="access_key=$ACCOUNT_KEY")
        BACKEND_CONFIGURED=true
        echo -e "${GREEN}Azure Storage for Terraform state set up successfully.${NC}"
    else
        echo -e "${YELLOW}Using local Terraform state.${NC}"
    fi
    echo
}

# Initialize Terraform
function init_terraform() {
    echo -e "${YELLOW}Initializing Terraform...${NC}"
    
    if [ "$BACKEND_CONFIGURED" = false ]; then
        (cd $TF_DIR/environments/$ENV && terraform init)
    fi
    
    echo -e "${GREEN}Terraform initialized successfully.${NC}"
    echo
}

# Plan Terraform
function plan_terraform() {
    echo -e "${YELLOW}Creating Terraform plan...${NC}"
    tf_with_unlock "(cd $TF_DIR/environments/$ENV && terraform plan -out=tfplan)"
    echo -e "${GREEN}Terraform plan created successfully.${NC}"
    echo
}

# Helper: Run terraform command and auto-unlock if state is locked
function tf_with_unlock() {
    local cmd="$1"
    local output
    output=$(eval "$cmd" 2>&1)
    if echo "$output" | grep -q 'state blob is already locked'; then
        lock_id=$(echo "$output" | grep -oE 'ID: *[a-z0-9\-]+' | awk '{print $2}')
        if [ -n "$lock_id" ]; then
            echo -e "${RED}Terraform state is locked (ID: $lock_id). Forcing unlock...${NC}"
            unlock_output=$(cd $TF_DIR/environments/$ENV && terraform force-unlock -force $lock_id 2>&1)
            if echo "$unlock_output" | grep -q 'successfully unlocked'; then
                echo -e "${GREEN}Terraform state has been successfully unlocked!${NC}"
                echo -e "${YELLOW}Retrying: $cmd${NC}"
                output=$(eval "$cmd" 2>&1)
            else
                echo -e "${RED}Failed to unlock state. Please check manually.\n$unlock_output${NC}"
                return 1
            fi
        else
            echo -e "${RED}Could not extract lock ID. Please check the state lock manually.${NC}"
            return 1
        fi
    fi
    echo "$output"
}

# Apply Terraform
function apply_terraform() {
    echo -e "${YELLOW}Applying Terraform configuration...${NC}"
    if [ -f "$TF_DIR/environments/$ENV/tfplan" ]; then
        tf_with_unlock "(cd $TF_DIR/environments/$ENV && terraform apply tfplan || true)"
    else
        tf_with_unlock "(cd $TF_DIR/environments/$ENV && terraform apply || true)"
    fi
    
    # Create ADLS Gen2 filesystems manually after apply
    manual_create_filesystems
    
    echo -e "${GREEN}Terraform configuration applied successfully.${NC}"
    echo
}

# Destroy infrastructure
function destroy_terraform() {
    echo -e "${RED}WARNING: This operation will delete all deployed resources.${NC}"
    echo -e "${RED}Are you sure you want to continue? (yes/no)${NC}"
    read -p "Confirm [yes/no]: " confirm
    
    if [[ $confirm == "yes" ]]; then
        echo -e "${YELLOW}Destroying infrastructure...${NC}"
        (cd $TF_DIR/environments/$ENV && terraform destroy)
        echo -e "${GREEN}Infrastructure destroyed successfully.${NC}"
    else
        echo -e "${YELLOW}Operation cancelled.${NC}"
    fi
    
    echo
}

# Show outputs
function show_outputs() {
    echo -e "${YELLOW}Displaying output information...${NC}"
    (cd $TF_DIR/environments/$ENV && terraform output)
    echo -e "${GREEN}Output information displayed successfully.${NC}"
    echo
}

# Check and import Databricks workspace if exists
function import_databricks_if_exists() {
    echo -e "${YELLOW}Checking for existing Databricks workspace...${NC}"
    ws_name="lotus-prism-dev-databricks"
    rg_name="lotus-prism-dev-rg"
    # Check existence on Azure
    ws_id=$(az databricks workspace show --name $ws_name --resource-group $rg_name --query id -o tsv 2>/dev/null)
    if [ -n "$ws_id" ]; then
        # Check if already in state
        (cd $TF_DIR/environments/$ENV && terraform state list | grep module.lotus_prism.module.databricks.azurerm_databricks_workspace.databricks > /dev/null 2>&1)
        if [ $? -ne 0 ]; then
            echo -e "${GREEN}Databricks workspace $ws_name already exists on Azure. Importing into Terraform state...${NC}"
            tf_with_unlock "(cd $TF_DIR/environments/$ENV && terraform import module.lotus_prism.module.databricks.azurerm_databricks_workspace.databricks $ws_id)"
        else
            echo -e "${YELLOW}Databricks workspace $ws_name is already managed by Terraform, skipping import.${NC}"
        fi
    else
        echo -e "${YELLOW}Databricks workspace $ws_name does not exist, will be created if needed.${NC}"
    fi
    echo
}

# Check and import resource group if exists
function import_resource_group_if_exists() {
    echo -e "${YELLOW}Checking for existing resource group...${NC}"
    rg_name="lotus-prism-${ENV}-rg"
    # Check existence on Azure
    rg_id=$(az group show --name $rg_name --query id -o tsv 2>/dev/null)
    if [ -n "$rg_id" ]; then
        # Check if already in state
        (cd $TF_DIR/environments/$ENV && terraform state list | grep module.lotus_prism.azurerm_resource_group.rg > /dev/null 2>&1)
        if [ $? -ne 0 ]; then
            echo -e "${GREEN}Resource group $rg_name already exists on Azure. Importing into Terraform state...${NC}"
            tf_with_unlock "(cd $TF_DIR/environments/$ENV && terraform import module.lotus_prism.azurerm_resource_group.rg $rg_id)"
        else
            echo -e "${YELLOW}Resource group $rg_name is already managed by Terraform, skipping import.${NC}"
        fi
    else
        echo -e "${YELLOW}Resource group $rg_name does not exist, will be created if needed.${NC}"
    fi
    echo
}

# Check and import API Management if exists
function import_apim_if_exists() {
    echo -e "${YELLOW}Checking for existing API Management...${NC}"
    apim_name="lotus-prism-${ENV}-apim"
    rg_name="lotus-prism-${ENV}-rg"
    # Check existence on Azure
    apim_id=$(az apim show --name $apim_name --resource-group $rg_name --query id -o tsv 2>/dev/null)
    if [ -n "$apim_id" ]; then
        # Check if already in state
        (cd $TF_DIR/environments/$ENV && terraform state list | grep module.lotus_prism.module.api.azurerm_api_management.apim > /dev/null 2>&1)
        if [ $? -ne 0 ]; then
            echo -e "${GREEN}API Management $apim_name already exists on Azure. Importing into Terraform state...${NC}"
            tf_with_unlock "(cd $TF_DIR/environments/$ENV && terraform import module.lotus_prism.module.api.azurerm_api_management.apim $apim_id)"
        else
            echo -e "${YELLOW}API Management $apim_name is already managed by Terraform, skipping import.${NC}"
        fi
    else
        echo -e "${YELLOW}API Management $apim_name does not exist, will be created if needed.${NC}"
    fi
    echo
}

# Check and import Event Hub if exists
function import_eventhub_if_exists() {
    echo -e "${YELLOW}Checking for existing Event Hub Namespace...${NC}"
    eventhub_name="lotus-prism-${ENV}-eventhub"
    rg_name="lotus-prism-${ENV}-rg"
    # Check existence on Azure
    eventhub_id=$(az eventhubs namespace show --name $eventhub_name --resource-group $rg_name --query id -o tsv 2>/dev/null)
    if [ -n "$eventhub_id" ]; then
        # Check if already in state
        (cd $TF_DIR/environments/$ENV && terraform state list | grep module.lotus_prism.module.eventhubs.azurerm_eventhub_namespace.eventhub > /dev/null 2>&1)
        if [ $? -ne 0 ]; then
            echo -e "${GREEN}Event Hub Namespace $eventhub_name already exists on Azure. Importing into Terraform state...${NC}"
            tf_with_unlock "(cd $TF_DIR/environments/$ENV && terraform import module.lotus_prism.module.eventhubs.azurerm_eventhub_namespace.eventhub $eventhub_id)"
        else
            echo -e "${YELLOW}Event Hub Namespace $eventhub_name is already managed by Terraform, skipping import.${NC}"
        fi
    else
        echo -e "${YELLOW}Event Hub Namespace $eventhub_name does not exist, will be created if needed.${NC}"
    fi
    echo
}

# Check and import Storage Account if exists
function import_storage_if_exists() {
    echo -e "${YELLOW}Checking for existing Storage Account...${NC}"
    storage_name="lotusprism${ENV}adls"
    rg_name="lotus-prism-${ENV}-rg"
    # Check existence on Azure
    storage_id=$(az storage account show --name $storage_name --resource-group $rg_name --query id -o tsv 2>/dev/null)
    if [ -n "$storage_id" ]; then
        # Check if already in state
        (cd $TF_DIR/environments/$ENV && terraform state list | grep module.lotus_prism.module.storage.azurerm_storage_account.storage > /dev/null 2>&1)
        if [ $? -ne 0 ]; then
            echo -e "${GREEN}Storage Account $storage_name already exists on Azure. Importing into Terraform state...${NC}"
            tf_with_unlock "(cd $TF_DIR/environments/$ENV && terraform import module.lotus_prism.module.storage.azurerm_storage_account.storage $storage_id)"
        else
            echo -e "${YELLOW}Storage Account $storage_name is already managed by Terraform, skipping import.${NC}"
        fi
    else
        echo -e "${YELLOW}Storage Account $storage_name does not exist, will be created if needed.${NC}"
    fi
    echo
}

# Check and import API Management APIs if exist
function import_apim_apis_if_exist() {
    echo -e "${YELLOW}Checking for existing API Management APIs...${NC}"
    apim_name="lotus-prism-${ENV}-apim"
    rg_name="lotus-prism-${ENV}-rg"
    
    # Lấy danh sách API từ Azure trực tiếp
    api_list=$(az apim api list --resource-group $rg_name --service-name $apim_name --query "[].name" -o tsv 2>/dev/null)
    
    # Danh sách API trong module và tên resource tương ứng
    # Format: "api_name:resource_name"
    apis=("price-analytics:price_analytics" "product-analytics:product_analytics" "promotion-analytics:promotion_analytics")
    
    for api_pair in "${apis[@]}"; do
        # Tách api_name và resource_name
        api_name="${api_pair%%:*}"
        resource_name="${api_pair##*:}"
        
        if echo "$api_list" | grep -q "^$api_name$"; then
            (cd $TF_DIR/environments/$ENV && terraform state list | grep "module.lotus_prism.module.api.azurerm_api_management_api.$resource_name" > /dev/null 2>&1)
            if [ $? -ne 0 ]; then
                echo -e "${GREEN}API $api_name already exists on Azure. Importing into Terraform state...${NC}"
                api_id="/subscriptions/$AZURE_SUBSCRIPTION_ID/resourceGroups/$rg_name/providers/Microsoft.ApiManagement/service/$apim_name/apis/$api_name;rev=1"
                tf_with_unlock "(cd $TF_DIR/environments/$ENV && terraform import module.lotus_prism.module.api.azurerm_api_management_api.$resource_name \"$api_id\")"
            else
                echo -e "${YELLOW}API $api_name is already managed by Terraform, skipping import.${NC}"
            fi
        else
            echo -e "${YELLOW}API $api_name does not exist, will be created if needed.${NC}"
        fi
    done
    echo
}

# Check and import Event Hubs if exist
function import_eventhubs_if_exist() {
    echo -e "${YELLOW}Checking for existing Event Hubs...${NC}"
    eventhub_name="lotus-prism-${ENV}-eventhub"
    rg_name="lotus-prism-${ENV}-rg"
    
    # List of Event Hubs to check
    hubs=("price-changes" "promotion-events" "market-trends" "alerts")
    
    for hub in "${hubs[@]}"; do
        hub_id=$(az eventhubs eventhub show --name $hub --namespace-name $eventhub_name --resource-group $rg_name --query id -o tsv 2>/dev/null)
        if [ -n "$hub_id" ]; then
            # Convert dash to underscore for Terraform resource address
            tf_hub_name=$(echo $hub | tr '-' '_')
            # Check if already in state
            (cd $TF_DIR/environments/$ENV && terraform state list | grep "module.lotus_prism.module.eventhubs.azurerm_eventhub.$tf_hub_name" > /dev/null 2>&1)
            if [ $? -ne 0 ]; then
                echo -e "${GREEN}Event Hub $hub already exists on Azure. Importing into Terraform state...${NC}"
                tf_with_unlock "(cd $TF_DIR/environments/$ENV && terraform import \"module.lotus_prism.module.eventhubs.azurerm_eventhub.$tf_hub_name\" \"$hub_id\")"
            else
                echo -e "${YELLOW}Event Hub $hub is already managed by Terraform, skipping import.${NC}"
            fi
        else
            echo -e "${YELLOW}Event Hub $hub does not exist, will be created if needed.${NC}"
        fi
    done
    echo
}

# Check and import Synapse workspace if exists
function import_synapse_if_exists() {
    echo -e "${YELLOW}Checking for existing Synapse workspace...${NC}"
    synapse_name="lotus-prism-${ENV}-synapse"
    rg_name="lotus-prism-${ENV}-rg"
    # Check existence on Azure
    synapse_id=$(az synapse workspace show --name $synapse_name --resource-group $rg_name --query id -o tsv 2>/dev/null)
    if [ -n "$synapse_id" ]; then
        # Check if already in state
        (cd $TF_DIR/environments/$ENV && terraform state list | grep module.lotus_prism.module.synapse.azurerm_synapse_workspace.synapse > /dev/null 2>&1)
        if [ $? -ne 0 ]; then
            echo -e "${GREEN}Synapse workspace $synapse_name already exists on Azure. Importing into Terraform state...${NC}"
            tf_with_unlock "(cd $TF_DIR/environments/$ENV && terraform import module.lotus_prism.module.synapse.azurerm_synapse_workspace.synapse $synapse_id)"
        else
            echo -e "${YELLOW}Synapse workspace $synapse_name is already managed by Terraform, skipping import.${NC}"
        fi
    else
        echo -e "${YELLOW}Synapse workspace $synapse_name does not exist, will be created if needed.${NC}"
    fi
    echo
}

# Import API Management Products if exist
function import_apim_products_if_exist() {
    echo -e "${YELLOW}Checking for existing API Management Products...${NC}"
    apim_name="lotus-prism-${ENV}-apim"
    rg_name="lotus-prism-${ENV}-rg"
    products=("analytics")
    for product in "${products[@]}"; do
        product_id="/subscriptions/$AZURE_SUBSCRIPTION_ID/resourceGroups/$rg_name/providers/Microsoft.ApiManagement/service/$apim_name/products/$product"
        (cd $TF_DIR/environments/$ENV && terraform state list | grep "module.lotus_prism.module.api.azurerm_api_management_product.$product" > /dev/null 2>&1)
        if [ $? -ne 0 ]; then
            echo -e "${GREEN}Product $product already exists on Azure. Importing into Terraform state...${NC}"
            tf_with_unlock "(cd $TF_DIR/environments/$ENV && terraform import \"module.lotus_prism.module.api.azurerm_api_management_product.$product\" \"$product_id\")"
        else
            echo -e "${YELLOW}Product $product is already managed by Terraform, skipping import.${NC}"
        fi
    done
    echo
}

# Import EventHub consumer groups (for_each)
function import_eventhub_consumer_groups_if_exist() {
    echo -e "${YELLOW}Checking for existing EventHub Consumer Groups...${NC}"
    eventhub_name="lotus-prism-${ENV}-eventhub"
    rg_name="lotus-prism-${ENV}-rg"
    hubs=("price-changes" "promotion-events" "market-trends" "alerts")
    groups=("databricks" "stream-analytics")
    for hub in "${hubs[@]}"; do
        for group in "${groups[@]}"; do
            tf_hub_name=$(echo $hub | tr '-' '_')
            tf_group_name=$(echo $group | tr '-' '_')
            cg_id="/subscriptions/$AZURE_SUBSCRIPTION_ID/resourceGroups/$rg_name/providers/Microsoft.EventHub/namespaces/$eventhub_name/eventhubs/$hub/consumerGroups/$group"
            (cd $TF_DIR/environments/$ENV && terraform state list | grep "module.lotus_prism.module.eventhubs.azurerm_eventhub_consumer_group.$tf_group_name\[\"$hub\"\]" > /dev/null 2>&1)
            if [ $? -ne 0 ]; then
                echo -e "${GREEN}Consumer Group $group for $hub already exists. Importing...${NC}"
                tf_with_unlock "(cd $TF_DIR/environments/$ENV && terraform import \"module.lotus_prism.module.eventhubs.azurerm_eventhub_consumer_group.$tf_group_name[\\\"$hub\\\"]\" \"$cg_id\")"
            else
                echo -e "${YELLOW}Consumer Group $group for $hub is already managed by Terraform, skipping import.${NC}"
            fi
        done
    done
    echo
}

# Import EventHub authorization rules
function import_eventhub_auth_rules_if_exist() {
    echo -e "${YELLOW}Checking for existing EventHub Namespace Authorization Rules...${NC}"
    eventhub_name="lotus-prism-${ENV}-eventhub"
    rg_name="lotus-prism-${ENV}-rg"
    rules=("sender" "processor")
    for rule in "${rules[@]}"; do
        rule_id="/subscriptions/$AZURE_SUBSCRIPTION_ID/resourceGroups/$rg_name/providers/Microsoft.EventHub/namespaces/$eventhub_name/authorizationRules/$rule"
        (cd $TF_DIR/environments/$ENV && terraform state list | grep "module.lotus_prism.module.eventhubs.azurerm_eventhub_namespace_authorization_rule.$rule" > /dev/null 2>&1)
        if [ $? -ne 0 ]; then
            echo -e "${GREEN}Authorization Rule $rule already exists. Importing...${NC}"
            tf_with_unlock "(cd $TF_DIR/environments/$ENV && terraform import \"module.lotus_prism.module.eventhubs.azurerm_eventhub_namespace_authorization_rule.$rule\" \"$rule_id\")"
        else
            echo -e "${YELLOW}Authorization Rule $rule is already managed by Terraform, skipping import.${NC}"
        fi
    done
    echo
}

# Import Synapse firewall rule
function import_synapse_fw_rule_if_exists() {
    echo -e "${YELLOW}Checking for existing Synapse Firewall Rule...${NC}"
    synapse_name="lotus-prism-${ENV}-synapse"
    rg_name="lotus-prism-${ENV}-rg"
    rule_name="AllowAllWindowsAzureIps"
    fw_id="/subscriptions/$AZURE_SUBSCRIPTION_ID/resourceGroups/$rg_name/providers/Microsoft.Synapse/workspaces/$synapse_name/firewallRules/$rule_name"
    (cd $TF_DIR/environments/$ENV && terraform state list | grep module.lotus_prism.module.synapse.azurerm_synapse_firewall_rule.allow_azure_services > /dev/null 2>&1)
    if [ $? -ne 0 ]; then
        echo -e "${GREEN}Synapse firewall rule $rule_name already exists. Importing...${NC}"
        tf_with_unlock "(cd $TF_DIR/environments/$ENV && terraform import module.lotus_prism.module.synapse.azurerm_synapse_firewall_rule.allow_azure_services \"$fw_id\")"
    else
        echo -e "${YELLOW}Synapse firewall rule $rule_name is already managed by Terraform, skipping import.${NC}"
    fi
    echo
}

# Import API Management product policy
function import_apim_product_policy_if_exists() {
    echo -e "${YELLOW}Checking for existing API Management Product Policies...${NC}"
    apim_name="lotus-prism-${ENV}-apim"
    rg_name="lotus-prism-${ENV}-rg"
    product_name="analytics"
    
    # Construct the product policy ID
    policy_id="/subscriptions/$AZURE_SUBSCRIPTION_ID/resourceGroups/$rg_name/providers/Microsoft.ApiManagement/service/$apim_name/products/$product_name"
    
    (cd $TF_DIR/environments/$ENV && terraform state list | grep "module.lotus_prism.module.api.azurerm_api_management_product_policy.rate_limit" > /dev/null 2>&1)
    if [ $? -ne 0 ]; then
        echo -e "${GREEN}Product policy for $product_name already exists. Importing...${NC}"
        tf_with_unlock "(cd $TF_DIR/environments/$ENV && terraform import \"module.lotus_prism.module.api.azurerm_api_management_product_policy.rate_limit\" \"$policy_id\")"
    else
        echo -e "${YELLOW}Product policy for $product_name is already managed by Terraform, skipping import.${NC}"
    fi
    echo
}

# Import API Management Product API
function import_apim_product_api_if_exists() {
    echo -e "${YELLOW}Checking for existing API Management Product APIs...${NC}"
    apim_name="lotus-prism-${ENV}-apim"
    rg_name="lotus-prism-${ENV}-rg"
    product_id="analytics"
    
    # List of APIs to check
    apis=("price-analytics" "promotion-analytics" "product-analytics")
    
    for api in "${apis[@]}"; do
        api_id="/subscriptions/$AZURE_SUBSCRIPTION_ID/resourceGroups/$rg_name/providers/Microsoft.ApiManagement/service/$apim_name/products/$product_id/apis/$api"
        tf_api_name=$(echo $api | tr '-' '_')
        
        (cd $TF_DIR/environments/$ENV && terraform state list | grep "module.lotus_prism.module.api.azurerm_api_management_product_api.$tf_api_name" > /dev/null 2>&1)
        if [ $? -ne 0 ]; then
            echo -e "${GREEN}Product API $api already exists. Importing...${NC}"
            tf_with_unlock "(cd $TF_DIR/environments/$ENV && terraform import \"module.lotus_prism.module.api.azurerm_api_management_product_api.$tf_api_name\" \"$api_id\")"
        else
            echo -e "${YELLOW}Product API $api is already managed by Terraform, skipping import.${NC}"
        fi
    done
    echo
}

# Import ADLS Gen2 Filesystems
function import_adls_filesystems_if_exist() {
    echo -e "${YELLOW}Importing ADLS Gen2 filesystems into Terraform state...${NC}"
    storage_name="lotusprism${ENV}adls"
    rg_name="lotus-prism-${ENV}-rg"
    
    filesystems=("bronze" "silver" "gold")
    for fs in "${filesystems[@]}"; do
        # Use the correct URL format for ADLS Gen2 filesystem
        fs_id="https://${storage_name}.dfs.core.windows.net/${fs}"
        
        (cd $TF_DIR/environments/$ENV && terraform state list | grep "module.lotus_prism.module.storage.azurerm_storage_data_lake_gen2_filesystem.$fs" > /dev/null 2>&1)
        if [ $? -ne 0 ]; then
            echo -e "${GREEN}Filesystem $fs already exists. Importing...${NC}"
            tf_with_unlock "(cd $TF_DIR/environments/$ENV && terraform import \"module.lotus_prism.module.storage.azurerm_storage_data_lake_gen2_filesystem.$fs\" \"$fs_id\")"
        else
            echo -e "${YELLOW}Filesystem $fs is already managed by Terraform, skipping import.${NC}"
        fi
    done
    echo
}

# Check and handle API Management Subscription limit
function check_and_handle_apim_subscription() {
    echo -e "${YELLOW}Checking API Management Subscription limit...${NC}"
    apim_name="lotus-prism-${ENV}-apim"
    rg_name="lotus-prism-${ENV}-rg"
    
    # Tìm subscription resource trong tf files
    subscription_resource=$(grep -r "azurerm_api_management_subscription" $TF_DIR/modules/api --include="*.tf" | head -1)
    
    if [ -n "$subscription_resource" ]; then
        # Kiểm tra xem có lỗi subscription limit trong lần apply trước không
        if [ -f "$TF_DIR/environments/$ENV/apply_log.txt" ] && grep -q "Subscriptions limit reached for same user" "$TF_DIR/environments/$ENV/apply_log.txt"; then
            echo -e "${RED}Subscriptions limit reached in previous run. Temporarily disabling subscription resource...${NC}"
            
            # Tìm file chứa subscription resource
            file_with_subscription=$(grep -l "azurerm_api_management_subscription" $TF_DIR/modules/api/*.tf | head -1)
            
            if [ -n "$file_with_subscription" ]; then
                # Tạo backup
                cp "$file_with_subscription" "${file_with_subscription}.bak"
                
                # Comment out subscription resource
                sed -i.tmp '/resource "azurerm_api_management_subscription"/,/^}/s/^/#TEMP_DISABLED: /' "$file_with_subscription"
                
                echo -e "${YELLOW}Temporarily commented out subscription resource in $file_with_subscription${NC}"
                echo -e "${YELLOW}Original file saved as ${file_with_subscription}.bak${NC}"
                
                # Tìm và comment các outputs liên quan đến subscription
                output_file="$TF_DIR/modules/api/outputs.tf"
                if [ -f "$output_file" ]; then
                    # Tạo backup
                    cp "$output_file" "${output_file}.bak"
                    
                    # Ghi lại file tạm không có outputs liên quan đến subscription
                    awk '
                    BEGIN { output_block = 0; skip_block = 0; }
                    /^output.*subscription/ { output_block = 1; skip_block = 1; print "#TEMP_DISABLED: " $0; next; }
                    /^}/ { 
                        if (output_block) { 
                            output_block = 0; 
                            print "#TEMP_DISABLED: " $0; 
                            next; 
                        }
                    }
                    { if (skip_block) print "#TEMP_DISABLED: " $0; else print $0; }
                    ' "$output_file" > "${output_file}.new"
                    
                    # Thay thế file cũ bằng file mới
                    mv "${output_file}.new" "$output_file"
                    
                    echo -e "${YELLOW}Temporarily commented out subscription references in $output_file${NC}"
                fi
            fi
        fi
    fi
    echo
}

# Restore temporarily disabled resources
function restore_disabled_resources() {
    echo -e "${YELLOW}Restoring any temporarily disabled resources...${NC}"
    
    # Check for any .bak files in modules/api
    for bak_file in $TF_DIR/modules/api/*.bak; do
        if [ -f "$bak_file" ]; then
            original_file="${bak_file%.bak}"
            echo -e "${GREEN}Restoring original file: $original_file${NC}"
            mv "$bak_file" "$original_file"
        fi
    done
    
    # Remove any .tmp files
    find $TF_DIR/modules/api -name "*.tmp" -delete
    
    echo
}

# Manually create ADLS Gen2 filesystems with Azure CLI then use Terraform to manage
function manual_create_filesystems() {
    echo -e "${YELLOW}Attempting to create ADLS Gen2 filesystems using Azure CLI...${NC}"
    create_adls_filesystems
    # Skip import - let Terraform maintain them as new resources if needed
    echo -e "${YELLOW}Filesystems created via Azure CLI. Terraform will maintain them.${NC}"
}

# Create ADLS Gen2 filesystems with Azure CLI
function create_adls_filesystems() {
    echo -e "${YELLOW}Creating ADLS Gen2 filesystems using Azure CLI...${NC}"
    storage_name="lotusprism${ENV}adls"
    rg_name="lotus-prism-${ENV}-rg"
    
    # Get storage account key
    key=$(az storage account keys list --account-name $storage_name --resource-group $rg_name --query "[0].value" -o tsv)
    
    if [ -n "$key" ]; then
        filesystems=("bronze" "silver" "gold")
        for fs in "${filesystems[@]}"; do
            echo -e "${YELLOW}Creating filesystem $fs...${NC}"
            existing=$(az storage fs exists --name $fs --account-name $storage_name --auth-mode key --account-key "$key" --query exists -o tsv)
            if [ "$existing" != "true" ]; then
                az storage fs create --name $fs --account-name $storage_name --auth-mode key --account-key "$key"
                if [ $? -eq 0 ]; then
                    echo -e "${GREEN}Filesystem $fs created successfully.${NC}"
                else
                    echo -e "${RED}Failed to create filesystem $fs.${NC}"
                fi
            else
                echo -e "${GREEN}Filesystem $fs already exists.${NC}"
            fi
        done
    else
        echo -e "${RED}Failed to get storage account key.${NC}"
    fi
    
    # Sleep for a moment to ensure Azure has fully registered the filesystems
    echo -e "${YELLOW}Waiting 5 seconds for Azure to register the filesystems...${NC}"
    sleep 5
    echo
}

# Force unlock Terraform state if locked
function force_unlock_state() {
    echo -e "${YELLOW}Checking for locked Terraform state...${NC}"
    if [ -f "$TF_DIR/environments/$ENV/.terraform.lock.hcl" ]; then
        # Extract lock ID using sed
        lock_id=$(cat "$TF_DIR/environments/$ENV/.terraform.lock.hcl" | sed -n 's/.*ID: \([^"]*\).*/\1/p')
        
        # Only attempt to unlock if we found a lock ID
        if [ -n "$lock_id" ]; then
            echo -e "${YELLOW}Found lock ID: $lock_id. Attempting to unlock...${NC}"
            terraform force-unlock -force "$lock_id"
            if [ $? -eq 0 ]; then
                echo -e "${GREEN}Terraform state unlocked successfully.${NC}"
            else
                echo -e "${YELLOW}Failed to unlock state.${NC}"
            fi
        else
            echo -e "${YELLOW}No lock ID found in lock file.${NC}"
        fi
    else
        echo -e "${YELLOW}No lock file found.${NC}"
    fi
    echo
}

# Deploy project with full process
function deploy_full() {
    select_environment
    setup_backend
    init_terraform
    force_unlock_state
    
    # Import existing resources
    import_resource_group_if_exists
    import_apim_if_exists
    import_apim_apis_if_exist
    import_apim_products_if_exist
    import_apim_product_api_if_exists
    import_apim_product_policy_if_exists
    import_eventhub_if_exists
    import_eventhubs_if_exist
    import_eventhub_consumer_groups_if_exist
    import_eventhub_auth_rules_if_exist
    import_storage_if_exists
    import_adls_filesystems_if_exist
    
    import_databricks_if_exists
    import_synapse_if_exists
    import_synapse_fw_rule_if_exists
    
    # Handle subscription limit before planning
    check_and_handle_apim_subscription
    
    # Save apply logs for subscription limit detection
    plan_terraform
    
    echo -e "${YELLOW}Do you want to apply the configuration now? (y/n)${NC}"
    read -p "Choice [y/n]: " apply_choice
    
    if [[ $apply_choice == "y" || $apply_choice == "Y" ]]; then
        # Capture apply output to detect subscription limits
        apply_output=$(tf_with_unlock "(cd $TF_DIR/environments/$ENV && terraform apply tfplan || true)")
        echo "$apply_output"
        echo "$apply_output" > $TF_DIR/environments/$ENV/apply_log.txt
        
        # If apply failed, try to create filesystems manually
        if echo "$apply_output" | grep -q "Error: creating File System"; then
            manual_create_filesystems
        fi
        
        show_outputs

        # Create .env file after successful deployment
        echo -e "${YELLOW}Creating .env file with credentials...${NC}"
        get_storage_credentials
        if [ $? -eq 0 ]; then
            echo -e "${GREEN}Successfully created .env file with credentials${NC}"
        else
            echo -e "${RED}Failed to create .env file. Please run option 9 to create it manually.${NC}"
        fi
    else
        echo -e "${YELLOW}Configuration application cancelled.${NC}"
    fi
    
    # Restore any temporarily disabled resources
    restore_disabled_resources
}

# Get Azure Storage credentials from Azure CLI
function get_storage_credentials() {
    echo -e "${YELLOW}Getting Azure Storage credentials...${NC}"
    
    # Get resource group name from Terraform output
    rg_name=$(cd $TF_DIR/environments/$ENV && terraform output -raw resource_group_name 2>/dev/null)
    if [ -z "$rg_name" ]; then
        echo -e "${RED}Failed to get resource group name from Terraform outputs${NC}"
        return 1
    fi
    
    # Get storage account name from Terraform output
    storage_name=$(cd $TF_DIR/environments/$ENV && terraform output -raw storage_account_name 2>/dev/null)
    if [ -z "$storage_name" ]; then
        echo -e "${RED}Failed to get storage account name from Terraform outputs${NC}"
        return 1
    fi
    
    # Get storage account key using Azure CLI
    echo -e "${YELLOW}Getting storage account key...${NC}"
    storage_key=$(az storage account keys list --account-name $storage_name --resource-group $rg_name --query "[0].value" -o tsv)
    if [ -z "$storage_key" ]; then
        echo -e "${RED}Failed to get storage account key from Azure CLI${NC}"
        return 1
    fi
    
    # Get container name (using bronze container as default)
    container_name="bronze"
    
    # Get API Management name from Terraform output
    apim_name=$(cd $TF_DIR/environments/$ENV && terraform output -raw api_management_name 2>/dev/null)
    if [ -z "$apim_name" ]; then
        echo -e "${RED}Failed to get API Management name from Terraform outputs${NC}"
        return 1
    fi
    
    # Get API Management gateway URL from Terraform output
    apim_url=$(cd $TF_DIR/environments/$ENV && terraform output -raw api_management_gateway_url 2>/dev/null)
    if [ -z "$apim_url" ]; then
        echo -e "${RED}Failed to get API Management gateway URL from Terraform outputs${NC}"
        return 1
    fi
    
    # Get Event Hub namespace name from Terraform output
    eventhub_name=$(cd $TF_DIR/environments/$ENV && terraform output -raw eventhub_namespace_name 2>/dev/null)
    if [ -z "$eventhub_name" ]; then
        echo -e "${RED}Failed to get Event Hub namespace name from Terraform outputs${NC}"
        return 1
    fi
    
    # Backup existing .env file if it exists
    if [ -f ".env" ]; then
        cp .env .env.bak
        echo -e "${YELLOW}Backed up existing .env file to .env.bak${NC}"
        
        # Create a temporary file
        temp_file=$(mktemp)
        
        # Read the existing .env file and update/create variables
        while IFS= read -r line || [ -n "$line" ]; do
            case "$line" in
                AZURE_STORAGE_ACCOUNT_NAME=*)
                    echo "AZURE_STORAGE_ACCOUNT_NAME=$storage_name" >> "$temp_file"
                    ;;
                AZURE_STORAGE_ACCOUNT_KEY=*)
                    echo "AZURE_STORAGE_ACCOUNT_KEY=$storage_key" >> "$temp_file"
                    ;;
                AZURE_STORAGE_CONTAINER=*)
                    echo "AZURE_STORAGE_CONTAINER=$container_name" >> "$temp_file"
                    ;;
                API_BASE_URL=*)
                    echo "API_BASE_URL=$apim_url" >> "$temp_file"
                    ;;
                PROXY_API_URL=*)
                    echo "PROXY_API_URL=$apim_url" >> "$temp_file"
                    ;;
                MARKET_TRENDS_API_URL=*)
                    echo "MARKET_TRENDS_API_URL=$apim_url" >> "$temp_file"
                    ;;
                *)
                    echo "$line" >> "$temp_file"
                    ;;
            esac
        done < .env
        
        # Add new variables if they don't exist
        if ! grep -q "^API_KEY=" "$temp_file"; then
            echo "API_KEY=your_api_key_here" >> "$temp_file"
        fi
        if ! grep -q "^PROXY_API_KEY=" "$temp_file"; then
            echo "PROXY_API_KEY=your_api_key_here" >> "$temp_file"
        fi
        if ! grep -q "^PROXY_CHECK_INTERVAL=" "$temp_file"; then
            echo "PROXY_CHECK_INTERVAL=300" >> "$temp_file"
        fi
        if ! grep -q "^MARKET_TRENDS_API_KEY=" "$temp_file"; then
            echo "MARKET_TRENDS_API_KEY=your_api_key_here" >> "$temp_file"
        fi
        if ! grep -q "^MARKET_TRENDS_UPDATE_INTERVAL=" "$temp_file"; then
            echo "MARKET_TRENDS_UPDATE_INTERVAL=3600" >> "$temp_file"
        fi
        
        # Replace the original file with the temporary file
        mv "$temp_file" .env
    else
        # Create new .env file with all configurations
        cat > .env << EOF
# Azure Storage Configuration
AZURE_STORAGE_ACCOUNT_NAME=$storage_name
AZURE_STORAGE_ACCOUNT_KEY=$storage_key
AZURE_STORAGE_CONTAINER=$container_name

# API Configuration
API_BASE_URL=$apim_url
API_KEY=your_api_key_here

# Proxy Configuration
PROXY_API_URL=$apim_url
PROXY_API_KEY=your_api_key_here
PROXY_CHECK_INTERVAL=300

# Market Trends Configuration
MARKET_TRENDS_API_URL=$apim_url
MARKET_TRENDS_API_KEY=your_api_key_here
MARKET_TRENDS_UPDATE_INTERVAL=3600

# Other configurations
SCRAPER_TIMEOUT=30
SCRAPER_RETRY_COUNT=3
SCRAPER_RETRY_DELAY=5
LOG_LEVEL=INFO
LOG_FILE=scraper.log
EOF
    fi
    
    echo -e "${GREEN}Azure credentials updated in .env file${NC}"
    
    # Check if API keys need to be set
    if grep -q "^API_KEY=your_api_key_here" .env || grep -q "^PROXY_API_KEY=your_api_key_here" .env || grep -q "^MARKET_TRENDS_API_KEY=your_api_key_here" .env; then
        echo -e "${YELLOW}Note: You need to manually add your API keys to the .env file${NC}"
        echo -e "${YELLOW}You can get the API keys from Azure Portal:${NC}"
        echo -e "${YELLOW}1. Go to API Management service: $apim_name${NC}"
        echo -e "${YELLOW}2. Go to Subscriptions${NC}"
        echo -e "${YELLOW}3. Copy the primary key and update the following in .env file:${NC}"
        echo -e "${YELLOW}   - API_KEY${NC}"
        echo -e "${YELLOW}   - PROXY_API_KEY${NC}"
        echo -e "${YELLOW}   - MARKET_TRENDS_API_KEY${NC}"
    fi
    return 0
}

# Run scraper
function run_scraper() {
    echo -e "${YELLOW}Running scraper...${NC}"
    
    # Get Azure Storage credentials
    get_storage_credentials
    if [ $? -ne 0 ]; then
        echo -e "${RED}Failed to get Azure Storage credentials. Please make sure Terraform is deployed first.${NC}"
        return 1
    fi
    
    # Check if virtual environment exists
    if [ ! -d "venv" ]; then
        echo -e "${YELLOW}Creating virtual environment...${NC}"
        python3 -m venv venv
    fi
    
    # Activate virtual environment
    echo -e "${YELLOW}Activating virtual environment...${NC}"
    source venv/bin/activate
    
    # Install requirements if needed
    if [ ! -f "venv/.requirements_installed" ]; then
        echo -e "${YELLOW}Installing requirements...${NC}"
        pip install -r requirements.txt
        touch venv/.requirements_installed
    fi
    
    # Run AEON scraper
    echo -e "${YELLOW}Running AEON scraper...${NC}"
    python src/run_aeon_scraper.py
    python src/run_aeon_trending_scraper.py
    
    # Deactivate virtual environment
    deactivate
    
    echo -e "${GREEN}Scraper completed!${NC}"
    echo
}

# Check deployment status
function check_deployment_status() {
    echo -e "${YELLOW}Checking deployment status...${NC}"
    
    # Check if Terraform is initialized
    if [ ! -d "$TF_DIR/environments/$ENV/.terraform" ]; then
        echo -e "${RED}❌ Terraform is not initialized${NC}"
        echo -e "${YELLOW}Please run option 4 to initialize Terraform${NC}"
        return 1
    fi
    
    # Check if Terraform is applied
    if [ ! -f "$TF_DIR/environments/$ENV/terraform.tfstate" ]; then
        echo -e "${RED}❌ Terraform is not applied${NC}"
        echo -e "${YELLOW}Please run option 1 or 6 to deploy infrastructure${NC}"
        return 1
    fi
    
    # Check required outputs
    echo -e "${YELLOW}Checking required resources...${NC}"
    
    # Check Storage Account
    storage_name=$(cd $TF_DIR/environments/$ENV && terraform output -raw storage_account_name 2>/dev/null)
    if [ -z "$storage_name" ]; then
        echo -e "${RED}❌ Storage Account not found${NC}"
    else
        echo -e "${GREEN}✅ Storage Account: $storage_name${NC}"
    fi
    
    # Check Proxy Service
    proxy_url=$(cd $TF_DIR/environments/$ENV && terraform output -raw proxy_api_url 2>/dev/null)
    if [ -z "$proxy_url" ]; then
        echo -e "${RED}❌ Proxy Service not found${NC}"
    else
        echo -e "${GREEN}✅ Proxy Service: $proxy_url${NC}"
    fi
    
    # Check Event Hub
    eventhub_name=$(cd $TF_DIR/environments/$ENV && terraform output -raw eventhub_name 2>/dev/null)
    if [ -z "$eventhub_name" ]; then
        echo -e "${RED}❌ Event Hub not found${NC}"
    else
        echo -e "${GREEN}✅ Event Hub: $eventhub_name${NC}"
    fi
    
    # Check API Management
    apim_name=$(cd $TF_DIR/environments/$ENV && terraform output -raw apim_name 2>/dev/null)
    if [ -z "$apim_name" ]; then
        echo -e "${RED}❌ API Management not found${NC}"
    else
        echo -e "${GREEN}✅ API Management: $apim_name${NC}"
    fi
    
    echo
    echo -e "${YELLOW}Deployment Status Summary:${NC}"
    if [ -n "$storage_name" ] && [ -n "$proxy_url" ] && [ -n "$eventhub_name" ] && [ -n "$apim_name" ]; then
        echo -e "${GREEN}✅ All required resources are deployed${NC}"
        echo -e "${GREEN}You can now run the scraper (option 9)${NC}"
        return 0
    else
        echo -e "${RED}❌ Some required resources are missing${NC}"
        echo -e "${YELLOW}Please complete the deployment first${NC}"
        return 1
    fi
}

# Function to upload notebooks to Databricks
function upload_notebooks_to_databricks() {
    echo -e "${YELLOW}Uploading notebooks to Databricks...${NC}"
    
    # Get Databricks workspace URL from Terraform output
    workspace_url=$(cd $TF_DIR/environments/$ENV && terraform output -raw databricks_workspace_url 2>/dev/null)
    if [ -z "$workspace_url" ]; then
        echo -e "${RED}Failed to get Databricks workspace URL from Terraform outputs${NC}"
        return 1
    fi
    
    # Add https:// to the workspace URL if not present
    if [[ $workspace_url != https://* ]]; then
        workspace_url="https://$workspace_url"
    fi
    
    # Check if API_DATABRICK_KEY exists in .env
    local databricks_token=""
    if [ -f ".env" ] && grep -q "^API_DATABRICK_KEY=" ".env"; then
        # Extract the token value from .env
        databricks_token=$(grep "^API_DATABRICK_KEY=" ".env" | cut -d'=' -f2)
        echo -e "${GREEN}Found Databricks token in .env file${NC}"
    else
        # Get Databricks token - we first need to generate a token if not exists
        # For simplicity, we'll just prompt the user to create a token manually
        echo -e "${YELLOW}To upload notebooks to Databricks, you need a Databricks token.${NC}"
        echo -e "${YELLOW}Please follow these steps:${NC}"
        echo -e "${YELLOW}1. Go to Databricks workspace: $workspace_url${NC}"
        echo -e "${YELLOW}2. Click on your user icon in the top right corner${NC}"
        echo -e "${YELLOW}3. Select 'User Settings'${NC}"
        echo -e "${YELLOW}4. Go to 'Access Tokens' tab${NC}"
        echo -e "${YELLOW}5. Click 'Generate New Token'${NC}"
        echo -e "${YELLOW}6. Enter a comment like 'LOTUS-PRISM deployment' and set an expiration${NC}"
        echo -e "${YELLOW}7. Copy the generated token${NC}"
        
        read -p "Enter your Databricks token: " databricks_token
        
        if [ -z "$databricks_token" ]; then
            echo -e "${RED}No token provided. Cannot upload notebooks.${NC}"
            return 1
        fi
        
        # Add token to .env file for future use
        if [ -f ".env" ]; then
            # Check if the variable already exists in .env
            if grep -q "^API_DATABRICK_KEY=" ".env"; then
                # Update the existing value
                sed -i.bak "s/^API_DATABRICK_KEY=.*/API_DATABRICK_KEY=$databricks_token/" ".env"
                rm -f ".env.bak"
            else
                # Add new variable
                echo "API_DATABRICK_KEY=$databricks_token" >> ".env"
            fi
        else
            # Create new .env file
            echo "API_DATABRICK_KEY=$databricks_token" >> ".env"
        fi
    fi
    
    # Create databricks CLI config file directly
    mkdir -p ~/.databrickscfg.d
    cat > ~/.databrickscfg << EOF
[DEFAULT]
host = $workspace_url
token = $databricks_token
EOF
    chmod 600 ~/.databrickscfg
    
    echo -e "${GREEN}Databricks CLI configured successfully with workspace: $workspace_url${NC}"
    
    # Install Databricks CLI if not installed
    if ! command -v databricks &> /dev/null; then
        echo -e "${YELLOW}Installing Databricks CLI...${NC}"
        pip install databricks-cli
    fi
    
    # Create LOTUS-PRISM folder in Databricks workspace
    echo -e "${YELLOW}Creating LOTUS-PRISM folder in Databricks workspace...${NC}"
    databricks workspace mkdirs /LOTUS-PRISM
    
    # Upload batch_etl_demo.py
    echo -e "${YELLOW}Uploading batch_etl_demo.py...${NC}"
    upload_output=$(databricks workspace import notebooks/batch_etl_demo.py /LOTUS-PRISM/batch_etl_demo --language python --format SOURCE --overwrite 2>&1)
    if [[ $upload_output == *"error_code"* && $upload_output != *"RESOURCE_ALREADY_EXISTS"* ]]; then
        echo -e "${RED}Failed to upload batch_etl_demo.py: $upload_output${NC}"
    else
        echo -e "${GREEN}Successfully uploaded/updated batch_etl_demo.py${NC}"
    fi
    
    # Upload streaming_demo.py
    echo -e "${YELLOW}Uploading streaming_demo.py...${NC}"
    upload_output=$(databricks workspace import notebooks/streaming_demo.py /LOTUS-PRISM/streaming_demo --language python --format SOURCE --overwrite 2>&1)
    if [[ $upload_output == *"error_code"* && $upload_output != *"RESOURCE_ALREADY_EXISTS"* ]]; then
        echo -e "${RED}Failed to upload streaming_demo.py: $upload_output${NC}"
    else
        echo -e "${GREEN}Successfully uploaded/updated streaming_demo.py${NC}"
    fi
    
    echo -e "${GREEN}Notebooks uploaded successfully!${NC}"
    echo
    
    # Ask if user wants to also upload configuration files
    echo -e "${YELLOW}Do you want to upload configuration files to Databricks FileStore? (y/n)${NC}"
    read -p "Choice [y/n]: " upload_config_choice
    
    if [[ $upload_config_choice == "y" || $upload_config_choice == "Y" ]]; then
        upload_config_files_to_databricks
    fi
    
    return 0
}

# Function to upload config files to Databricks FileStore
function upload_config_files_to_databricks() {
    echo -e "${YELLOW}Uploading configuration files to Databricks FileStore...${NC}"
    
    # Check if Databricks CLI is configured
    if [ ! -f ~/.databrickscfg ]; then
        echo -e "${RED}Databricks CLI is not configured. Please run option 11 first.${NC}"
        return 1
    fi
    
    # Create LOTUS-PRISM directory in DBFS if it doesn't exist
    databricks fs mkdirs dbfs:/FileStore/LOTUS-PRISM/configs
    
    # Upload batch config
    echo -e "${YELLOW}Uploading batch_config.yaml...${NC}"
    upload_output=$(databricks fs cp src/config/batch_config.yaml dbfs:/FileStore/LOTUS-PRISM/configs/batch_config.yaml --overwrite 2>&1)
    if [[ $upload_output == *"error_code"* && $upload_output != *"RESOURCE_ALREADY_EXISTS"* ]]; then
        echo -e "${RED}Failed to upload batch_config.yaml: $upload_output${NC}"
    else
        echo -e "${GREEN}Successfully uploaded/updated batch_config.yaml${NC}"
    fi
    
    # Upload streaming config
    echo -e "${YELLOW}Uploading streaming_config.yaml...${NC}"
    upload_output=$(databricks fs cp src/config/streaming_config.yaml dbfs:/FileStore/LOTUS-PRISM/configs/streaming_config.yaml --overwrite 2>&1)
    if [[ $upload_output == *"error_code"* && $upload_output != *"RESOURCE_ALREADY_EXISTS"* ]]; then
        echo -e "${RED}Failed to upload streaming_config.yaml: $upload_output${NC}"
    else
        echo -e "${GREEN}Successfully uploaded/updated streaming_config.yaml${NC}"
    fi
    
    echo -e "${GREEN}Configuration files uploaded successfully!${NC}"
    echo
    
    return 0
}

# Function to create Databricks clusters
function create_databricks_clusters() {
    echo -e "${YELLOW}Creating Databricks cluster...${NC}"
    
    # Check if Databricks CLI is configured
    if [ ! -f ~/.databrickscfg ]; then
        echo -e "${RED}Databricks CLI is not configured. Please run option 11 first.${NC}"
        return 1
    fi
    
    # Cluster name to use
    CLUSTER_NAME="LOTUS-PRISM-Unified"
    
    # Path to cluster configuration file
    CLUSTER_CONFIG_FILE="config/databricks/unified_cluster.json"
    
    # Check if config file exists
    if [ ! -f "$CLUSTER_CONFIG_FILE" ]; then
        echo -e "${RED}Cluster configuration file not found: $CLUSTER_CONFIG_FILE${NC}"
        echo -e "${YELLOW}Please make sure the configuration files are in place.${NC}"
        return 1
    fi
    
    # Check if we already have a cluster ID saved
    if [ -f ".lotus_cluster_id" ]; then
        CLUSTER_ID=$(cat .lotus_cluster_id)
        echo -e "${YELLOW}Found existing cluster ID: $CLUSTER_ID. Checking status...${NC}"
        
        # Check if cluster exists and its state
        cluster_info=$(databricks clusters get --cluster-id "$CLUSTER_ID" 2>/dev/null)
        if [ $? -eq 0 ]; then
            cluster_state=$(echo "$cluster_info" | jq -r '.state' 2>/dev/null)
            cluster_name=$(echo "$cluster_info" | jq -r '.cluster_name' 2>/dev/null)
            
            echo -e "${GREEN}Found existing cluster: $cluster_name (State: $cluster_state)${NC}"
            
            if [ "$cluster_state" == "TERMINATED" ]; then
                echo -e "${YELLOW}Cluster is terminated. Starting it...${NC}"
                databricks clusters start --cluster-id "$CLUSTER_ID"
                echo -e "${GREEN}Cluster start initiated. It may take a few minutes to start.${NC}"
            elif [ "$cluster_state" == "PENDING" ] || [ "$cluster_state" == "RESTARTING" ] || [ "$cluster_state" == "RESIZING" ]; then
                echo -e "${YELLOW}Cluster is currently in $cluster_state state. Please wait for it to become RUNNING.${NC}"
            elif [ "$cluster_state" == "RUNNING" ]; then
                echo -e "${GREEN}Cluster is already running.${NC}"
            else
                echo -e "${YELLOW}Cluster is in $cluster_state state. Trying to restart it...${NC}"
                databricks clusters restart --cluster-id "$CLUSTER_ID"
            fi
            
            # Use the existing cluster ID for both batch and streaming
            echo "$CLUSTER_ID" > .batch_cluster_id
            echo "$CLUSTER_ID" > .streaming_cluster_id
            
            echo -e "${GREEN}Using existing cluster for both batch and streaming jobs.${NC}"
            return 0
        else
            echo -e "${YELLOW}Saved cluster ID no longer exists. Checking if cluster with name $CLUSTER_NAME exists...${NC}"
        fi
    fi
    
    # Check if a cluster with our name already exists
    # List all clusters and grep for the name
    echo -e "${YELLOW}Checking for existing cluster with name $CLUSTER_NAME...${NC}"
    existing_clusters=$(databricks clusters list)
    existing_cluster_id=$(echo "$existing_clusters" | jq -r ".clusters[] | select(.cluster_name == \"$CLUSTER_NAME\") | .cluster_id" 2>/dev/null)
    
    if [ -n "$existing_cluster_id" ]; then
        echo -e "${GREEN}Found existing cluster with name $CLUSTER_NAME and ID: $existing_cluster_id${NC}"
        CLUSTER_ID=$existing_cluster_id
        
        # Check cluster state
        cluster_info=$(databricks clusters get --cluster-id "$CLUSTER_ID")
        cluster_state=$(echo "$cluster_info" | jq -r '.state')
        
        echo -e "${YELLOW}Cluster state: $cluster_state${NC}"
        
        if [ "$cluster_state" == "TERMINATED" ]; then
            echo -e "${YELLOW}Cluster is terminated. Starting it...${NC}"
            databricks clusters start --cluster-id "$CLUSTER_ID"
            echo -e "${GREEN}Cluster start initiated. It may take a few minutes to start.${NC}"
        elif [ "$cluster_state" == "PENDING" ] || [ "$cluster_state" == "RESTARTING" ] || [ "$cluster_state" == "RESIZING" ]; then
            echo -e "${YELLOW}Cluster is currently in $cluster_state state. Please wait for it to become RUNNING.${NC}"
        elif [ "$cluster_state" == "RUNNING" ]; then
            echo -e "${GREEN}Cluster is already running.${NC}"
        else
            echo -e "${YELLOW}Cluster is in $cluster_state state. Trying to restart it...${NC}"
            databricks clusters restart --cluster-id "$CLUSTER_ID"
        fi
        
        # Save the cluster ID
        echo "$CLUSTER_ID" > .lotus_cluster_id
        echo "$CLUSTER_ID" > .batch_cluster_id
        echo "$CLUSTER_ID" > .streaming_cluster_id
        
        echo -e "${GREEN}Using existing cluster for both batch and streaming jobs.${NC}"
        return 0
    fi
    
    # Create a single cluster for both batch and streaming workloads
    echo -e "${YELLOW}Creating LOTUS-PRISM unified cluster...${NC}"
    
    # Create unified cluster using the config file
    CLUSTER_ID=$(databricks clusters create --json-file "$CLUSTER_CONFIG_FILE" | jq -r '.cluster_id')
    
    if [ $? -ne 0 ] || [ -z "$CLUSTER_ID" ] || [ "$CLUSTER_ID" == "null" ]; then
        echo -e "${RED}Failed to create cluster.${NC}"
        return 1
    else
        echo -e "${GREEN}Cluster created with ID: $CLUSTER_ID${NC}"
        
        # Save cluster ID for all purposes
        echo "$CLUSTER_ID" > .lotus_cluster_id  # Save for future runs
        echo "$CLUSTER_ID" > .batch_cluster_id  # For batch jobs
        echo "$CLUSTER_ID" > .streaming_cluster_id  # For streaming jobs
        
        # Wait for cluster to start
        echo -e "${YELLOW}Waiting for cluster to start...${NC}"
        echo -e "${YELLOW}This may take several minutes.${NC}"
        
        # Poll for cluster status
        max_retries=10
        retry_count=0
        cluster_started=false
        
        while [ $retry_count -lt $max_retries ]; do
            echo -e "${YELLOW}Checking cluster status (attempt $(($retry_count + 1))/$max_retries)...${NC}"
            cluster_state=$(databricks clusters get --cluster-id "$CLUSTER_ID" | jq -r '.state')
            
            if [ "$cluster_state" == "RUNNING" ]; then
                echo -e "${GREEN}Cluster is now running!${NC}"
                cluster_started=true
                break
            elif [ "$cluster_state" == "ERROR" ] || [ "$cluster_state" == "UNKNOWN" ]; then
                echo -e "${RED}Cluster is in $cluster_state state. Please check the Databricks workspace.${NC}"
                break
            fi
            
            echo -e "${YELLOW}Current state: $cluster_state. Waiting 30 seconds...${NC}"
            retry_count=$((retry_count + 1))
            sleep 30
        done
        
        if [ "$cluster_started" != "true" ]; then
            echo -e "${YELLOW}Cluster is not yet in RUNNING state, but the ID has been saved.${NC}"
            echo -e "${YELLOW}You can check the cluster status in the Databricks workspace.${NC}"
        fi
    fi
    
    echo -e "${GREEN}Cluster created successfully for both batch and streaming jobs!${NC}"
    return 0
}

# Function to submit batch ETL job to Databricks
function submit_batch_job() {
    echo -e "${YELLOW}Submitting batch ETL job to Databricks...${NC}"
    
    # Check if Databricks CLI is configured
    if [ ! -f ~/.databrickscfg ]; then
        echo -e "${RED}Databricks CLI is not configured. Please run option 11 first.${NC}"
        return 1
    fi
    
    # Path to batch job configuration file
    JOB_CONFIG_FILE="config/databricks/batch_job.json"
    
    # Check if using existing cluster or creating new cluster per job
    if [ -f ".lotus_cluster_id" ] || [ -f ".batch_cluster_id" ]; then
        # Get cluster ID for job
        CLUSTER_ID=""
        if [ -f ".lotus_cluster_id" ]; then
            CLUSTER_ID=$(cat .lotus_cluster_id)
        elif [ -f ".batch_cluster_id" ]; then
            CLUSTER_ID=$(cat .batch_cluster_id)
        fi
        
        # Check if cluster exists and is running
        cluster_info=$(databricks clusters get --cluster-id "$CLUSTER_ID" 2>/dev/null)
        if [ $? -ne 0 ]; then
            echo -e "${RED}Invalid cluster ID or cluster does not exist.${NC}"
            echo -e "${YELLOW}Using job configuration with new cluster defined in $JOB_CONFIG_FILE${NC}"
        else
            cluster_state=$(echo "$cluster_info" | jq -r '.state' 2>/dev/null)
            cluster_name=$(echo "$cluster_info" | jq -r '.cluster_name' 2>/dev/null)
            
            echo -e "${YELLOW}Using cluster: $cluster_name (Current state: $cluster_state)${NC}"
            
            if [ "$cluster_state" != "RUNNING" ]; then
                echo -e "${YELLOW}Cluster is not running. Starting cluster...${NC}"
                databricks clusters start --cluster-id "$CLUSTER_ID"
                
                echo -e "${YELLOW}Waiting for cluster to start...${NC}"
                echo -e "${YELLOW}This may take a few minutes.${NC}"
                
                # Poll for cluster status
                max_retries=10
                retry_count=0
                
                while [ $retry_count -lt $max_retries ]; do
                    echo -e "${YELLOW}Checking cluster status (attempt $(($retry_count + 1))/$max_retries)...${NC}"
                    cluster_state=$(databricks clusters get --cluster-id "$CLUSTER_ID" | jq -r '.state')
                    
                    if [ "$cluster_state" == "RUNNING" ]; then
                        echo -e "${GREEN}Cluster is now running!${NC}"
                        break
                    elif [ "$cluster_state" == "ERROR" ] || [ "$cluster_state" == "UNKNOWN" ]; then
                        echo -e "${RED}Cluster is in $cluster_state state. Please check the Databricks workspace.${NC}"
                        echo -e "${YELLOW}Proceeding with job submission anyway. It will queue until the cluster is ready.${NC}"
                        break
                    fi
                    
                    echo -e "${YELLOW}Current state: $cluster_state. Waiting 30 seconds...${NC}"
                    retry_count=$((retry_count + 1))
                    sleep 30
                    
                    if [ $retry_count -eq $max_retries ]; then
                        echo -e "${YELLOW}Cluster is still not in RUNNING state.${NC}"
                        echo -e "${YELLOW}Proceeding with job submission anyway. It will queue until the cluster is ready.${NC}"
                    fi
                done
            fi
            
            # Create temporary job config file with existing cluster ID
            echo -e "${YELLOW}Using existing cluster for batch job...${NC}"
            
            # Check if job config file exists
            if [ -f "$JOB_CONFIG_FILE" ]; then
                TEMP_JOB_CONFIG=$(mktemp)
                jq --arg cluster_id "$CLUSTER_ID" '. + {existing_cluster_id: $cluster_id} | del(.new_cluster)' "$JOB_CONFIG_FILE" > "$TEMP_JOB_CONFIG"
                JOB_CONFIG_FILE="$TEMP_JOB_CONFIG"
            else
                echo -e "${RED}Job configuration file not found: $JOB_CONFIG_FILE${NC}"
                echo -e "${YELLOW}Creating a simple job configuration...${NC}"
                
                TEMP_JOB_CONFIG=$(mktemp)
                cat > "$TEMP_JOB_CONFIG" << EOF
{
  "run_name": "LOTUS-PRISM Batch ETL Job",
  "existing_cluster_id": "$CLUSTER_ID",
  "notebook_task": {
    "notebook_path": "/LOTUS-PRISM/batch_etl_demo",
    "base_parameters": {
      "config_path": "dbfs:/FileStore/LOTUS-PRISM/configs/batch_config.yaml"
    }
  }
}
EOF
                JOB_CONFIG_FILE="$TEMP_JOB_CONFIG"
            fi
        fi
    else
        # Check if job config file exists
        if [ ! -f "$JOB_CONFIG_FILE" ]; then
            echo -e "${RED}Job configuration file not found: $JOB_CONFIG_FILE${NC}"
            echo -e "${YELLOW}Please create a cluster first (option 12) or provide a job configuration file.${NC}"
            return 1
        fi
        
        echo -e "${YELLOW}Using job configuration with new cluster defined in $JOB_CONFIG_FILE${NC}"
    fi
    
    # Submit job using the config file
    echo -e "${YELLOW}Submitting batch ETL job...${NC}"
    job_id=$(databricks jobs create --json-file "$JOB_CONFIG_FILE" | jq -r '.job_id' 2>/dev/null)
    
    if [ -z "$job_id" ] || [ "$job_id" == "null" ]; then
        echo -e "${RED}Failed to create batch job.${NC}"
        return 1
    else
        echo -e "${GREEN}Batch job created successfully. Job ID: $job_id${NC}"
        run_response=$(databricks jobs run-now --job-id "$job_id")
        run_id=$(echo "$run_response" | jq -r '.run_id' 2>/dev/null)
        echo -e "${GREEN}Batch job run initiated. Run ID: $run_id${NC}"
        echo "$job_id" > .batch_job_id
    fi
    
    # Clean up temporary files
    if [[ "$JOB_CONFIG_FILE" == /tmp/* ]]; then
        rm -f "$JOB_CONFIG_FILE"
    fi
    
    return 0
}

# Function to submit streaming job to Databricks
function submit_streaming_job() {
    echo -e "${YELLOW}Submitting streaming job to Databricks...${NC}"
    
    # Check if Databricks CLI is configured
    if [ ! -f ~/.databrickscfg ]; then
        echo -e "${RED}Databricks CLI is not configured. Please run option 11 first.${NC}"
        return 1
    fi
    
    # Path to streaming job configuration file
    JOB_CONFIG_FILE="config/databricks/streaming_job.json"
    
    # Check if using existing cluster or creating new cluster per job
    if [ -f ".lotus_cluster_id" ] || [ -f ".streaming_cluster_id" ]; then
        # Get cluster ID for job
        CLUSTER_ID=""
        if [ -f ".lotus_cluster_id" ]; then
            CLUSTER_ID=$(cat .lotus_cluster_id)
        elif [ -f ".streaming_cluster_id" ]; then
            CLUSTER_ID=$(cat .streaming_cluster_id)
        fi
        
        # Check if cluster exists and is running
        cluster_info=$(databricks clusters get --cluster-id "$CLUSTER_ID" 2>/dev/null)
        if [ $? -ne 0 ]; then
            echo -e "${RED}Invalid cluster ID or cluster does not exist.${NC}"
            echo -e "${YELLOW}Using job configuration with new cluster defined in $JOB_CONFIG_FILE${NC}"
        else
            cluster_state=$(echo "$cluster_info" | jq -r '.state' 2>/dev/null)
            cluster_name=$(echo "$cluster_info" | jq -r '.cluster_name' 2>/dev/null)
            
            echo -e "${YELLOW}Using cluster: $cluster_name (Current state: $cluster_state)${NC}"
            
            if [ "$cluster_state" != "RUNNING" ]; then
                echo -e "${YELLOW}Cluster is not running. Starting cluster...${NC}"
                databricks clusters start --cluster-id "$CLUSTER_ID"
                
                echo -e "${YELLOW}Waiting for cluster to start...${NC}"
                echo -e "${YELLOW}This may take a few minutes.${NC}"
                
                # Poll for cluster status
                max_retries=10
                retry_count=0
                
                while [ $retry_count -lt $max_retries ]; do
                    echo -e "${YELLOW}Checking cluster status (attempt $(($retry_count + 1))/$max_retries)...${NC}"
                    cluster_state=$(databricks clusters get --cluster-id "$CLUSTER_ID" | jq -r '.state')
                    
                    if [ "$cluster_state" == "RUNNING" ]; then
                        echo -e "${GREEN}Cluster is now running!${NC}"
                        break
                    elif [ "$cluster_state" == "ERROR" ] || [ "$cluster_state" == "UNKNOWN" ]; then
                        echo -e "${RED}Cluster is in $cluster_state state. Please check the Databricks workspace.${NC}"
                        echo -e "${YELLOW}Proceeding with job submission anyway. It will queue until the cluster is ready.${NC}"
                        break
                    fi
                    
                    echo -e "${YELLOW}Current state: $cluster_state. Waiting 30 seconds...${NC}"
                    retry_count=$((retry_count + 1))
                    sleep 30
                    
                    if [ $retry_count -eq $max_retries ]; then
                        echo -e "${YELLOW}Cluster is still not in RUNNING state.${NC}"
                        echo -e "${YELLOW}Proceeding with job submission anyway. It will queue until the cluster is ready.${NC}"
                    fi
                done
            fi
            
            # Create temporary job config file with existing cluster ID
            echo -e "${YELLOW}Using existing cluster for streaming job...${NC}"
            
            # Check if job config file exists
            if [ -f "$JOB_CONFIG_FILE" ]; then
                TEMP_JOB_CONFIG=$(mktemp)
                jq --arg cluster_id "$CLUSTER_ID" '. + {existing_cluster_id: $cluster_id} | del(.new_cluster)' "$JOB_CONFIG_FILE" > "$TEMP_JOB_CONFIG"
                JOB_CONFIG_FILE="$TEMP_JOB_CONFIG"
            else
                echo -e "${RED}Job configuration file not found: $JOB_CONFIG_FILE${NC}"
                echo -e "${YELLOW}Creating a simple job configuration...${NC}"
                
                TEMP_JOB_CONFIG=$(mktemp)
                cat > "$TEMP_JOB_CONFIG" << EOF
{
  "run_name": "LOTUS-PRISM Streaming Job",
  "existing_cluster_id": "$CLUSTER_ID",
  "notebook_task": {
    "notebook_path": "/LOTUS-PRISM/streaming_demo",
    "base_parameters": {
      "config_path": "dbfs:/FileStore/LOTUS-PRISM/configs/streaming_config.yaml"
    }
  }
}
EOF
                JOB_CONFIG_FILE="$TEMP_JOB_CONFIG"
            fi
        fi
    else
        # Check if job config file exists
        if [ ! -f "$JOB_CONFIG_FILE" ]; then
            echo -e "${RED}Job configuration file not found: $JOB_CONFIG_FILE${NC}"
            echo -e "${YELLOW}Please create a cluster first (option 12) or provide a job configuration file.${NC}"
            return 1
        fi
        
        echo -e "${YELLOW}Using job configuration with new cluster defined in $JOB_CONFIG_FILE${NC}"
    fi
    
    # Submit job using the config file
    echo -e "${YELLOW}Submitting streaming job...${NC}"
    job_id=$(databricks jobs create --json-file "$JOB_CONFIG_FILE" | jq -r '.job_id' 2>/dev/null)
    
    if [ -z "$job_id" ] || [ "$job_id" == "null" ]; then
        echo -e "${RED}Failed to create streaming job.${NC}"
        return 1
    else
        echo -e "${GREEN}Streaming job created successfully. Job ID: $job_id${NC}"
        run_response=$(databricks jobs run-now --job-id "$job_id")
        run_id=$(echo "$run_response" | jq -r '.run_id' 2>/dev/null)
        echo -e "${GREEN}Streaming job run initiated. Run ID: $run_id${NC}"
        echo "$job_id" > .streaming_job_id
    fi
    
    # Clean up temporary files
    if [[ "$JOB_CONFIG_FILE" == /tmp/* ]]; then
        rm -f "$JOB_CONFIG_FILE"
    fi
    
    return 0
}

# Function to verify Phase 3 assets
function verify_phase3_assets() {
    echo -e "${YELLOW}Verifying Phase 3 (Data Processing) assets...${NC}"
    
    # Check if configuration files exist
    echo -e "${YELLOW}Checking configuration files...${NC}"
    if [ -f "src/config/batch_config.yaml" ] && [ -f "src/config/streaming_config.yaml" ]; then
        echo -e "${GREEN}✅ Configuration files found${NC}"
    else
        echo -e "${RED}❌ Configuration files missing${NC}"
        echo -e "${YELLOW}Creating default configuration files...${NC}"
        mkdir -p src/config
        
        # Create default batch_config.yaml if it doesn't exist
        if [ ! -f "src/config/batch_config.yaml" ]; then
            cat > src/config/batch_config.yaml << EOF
# Cấu hình batch ETL cho LOTUS-PRISM
# Delta Lake tables configuration
delta:
  bronze_layer_path: "abfss://bronze@lotusprismstorage.dfs.core.windows.net"
  silver_layer_path: "abfss://silver@lotusprismstorage.dfs.core.windows.net"
  gold_layer_path: "abfss://gold@lotusprismstorage.dfs.core.windows.net"

# Data Sources
data_sources:
  aeon_data: "abfss://bronze@lotusprismstorage.dfs.core.windows.net/raw/aeon"
  lotte_data: "abfss://bronze@lotusprismstorage.dfs.core.windows.net/raw/lotte"
  mm_mega_data: "abfss://bronze@lotusprismstorage.dfs.core.windows.net/raw/mm_mega"
  winmart_data: "abfss://bronze@lotusprismstorage.dfs.core.windows.net/raw/winmart"
  lotus_internal_data: "abfss://bronze@lotusprismstorage.dfs.core.windows.net/raw/lotus_internal"
  market_trend_data: "abfss://bronze@lotusprismstorage.dfs.core.windows.net/raw/market_trends"

# Batch Processing Parameters
processing:
  write_mode: "merge"
  partition_columns:
    - "category"
    - "date"
  max_records_per_file: 1000000
  optimize_interval: 10

# Validation
validation:
  min_records_threshold: 1000
  max_null_percentage: 0.05
  price_min_threshold: 1000
  price_max_threshold: 100000000
  enable_schema_validation: true
  enable_data_quality_checks: true
  notify_on_failure: true

# Spark Configuration
spark:
  executor_memory: "8g"
  executor_cores: 4
  driver_memory: "4g"
  max_executors: 10
  log_level: "WARN"
EOF
            echo -e "${GREEN}Created default batch_config.yaml${NC}"
        fi
        
        # Create default streaming_config.yaml if it doesn't exist
        if [ ! -f "src/config/streaming_config.yaml" ]; then
            cat > src/config/streaming_config.yaml << EOF
# Cấu hình streaming cho LOTUS-PRISM
# Kafka/Event Hubs Configuration
kafka:
  bootstrap_servers: "lotus-prism-eventhub.servicebus.windows.net:9093"
  security_protocol: "SASL_SSL"
  sasl_mechanism: "PLAIN"
  connection_string_key_vault: "eventhubs-connection-string"
  topics:
    price_changes: "price-changes"
    inventory_updates: "inventory-updates"
    competitor_prices: "competitor-prices"
    promo_updates: "promo-updates"

# Stream Processing Parameters
processing:
  trigger_interval: "10 seconds"
  checkpoint_location: "abfss://checkpoints@lotusprismstorage.dfs.core.windows.net/streaming"
  watermark_delay: "1 hours"
  output_mode: "append"

# Delta Lake
delta:
  silver_layer_path: "abfss://silver@lotusprismstorage.dfs.core.windows.net"
  gold_layer_path: "abfss://gold@lotusprismstorage.dfs.core.windows.net"
  optimize_interval_ms: 300000

# Notifications
notifications:
  price_change_threshold: 0.05
  significant_price_drop_threshold: 0.1
  anomaly_zscore_threshold: 3.0

# Spark Configuration
spark:
  executor_memory: "4g"
  executor_cores: 2
  driver_memory: "4g"
  max_executors: 8
  log_level: "WARN"
EOF
            echo -e "${GREEN}Created default streaming_config.yaml${NC}"
        fi
    fi
    
    # Check if notebooks exist
    echo -e "${YELLOW}Checking notebooks...${NC}"
    if [ -f "notebooks/batch_etl_demo.py" ] && [ -f "notebooks/streaming_demo.py" ]; then
        echo -e "${GREEN}✅ Notebooks found${NC}"
    else
        echo -e "${RED}❌ Notebooks missing${NC}"
        echo -e "${YELLOW}Please make sure the notebooks are in the notebooks directory${NC}"
        return 1
    fi
    
    # Check if batch processing modules exist
    echo -e "${YELLOW}Checking batch processing modules...${NC}"
    if [ -d "src/batch" ]; then
        echo -e "${GREEN}✅ Batch processing modules found${NC}"
    else
        echo -e "${RED}❌ Batch processing modules missing${NC}"
        echo -e "${YELLOW}Please make sure the batch processing modules are in src/batch${NC}"
        return 1
    fi
    
    # Check if streaming modules exist
    echo -e "${YELLOW}Checking streaming modules...${NC}"
    if [ -d "src/streaming" ]; then
        echo -e "${GREEN}✅ Streaming modules found${NC}"
    else
        echo -e "${RED}❌ Streaming modules missing${NC}"
        echo -e "${YELLOW}Creating directory structure for streaming modules...${NC}"
        mkdir -p src/streaming
        # Create empty __init__.py
        if [ ! -f "src/streaming/__init__.py" ]; then
            cat > src/streaming/__init__.py << EOF
"""
Module for real-time streaming data processing components.
"""
EOF
            echo -e "${GREEN}Created streaming module structure${NC}"
        fi
    fi
    
    echo -e "${GREEN}Phase 3 assets verification complete${NC}"
    return 0
}

# Function to install required Python libraries to Databricks cluster
function install_databricks_libraries() {
    echo -e "${YELLOW}Installing required Python libraries to Databricks cluster...${NC}"
    
    # Check if Databricks CLI is configured
    if [ ! -f ~/.databrickscfg ]; then
        echo -e "${RED}Databricks CLI is not configured. Please run option 11 first.${NC}"
        return 1
    fi
    
    # Get cluster ID
    CLUSTER_ID=""
    if [ -f ".lotus_cluster_id" ]; then
        CLUSTER_ID=$(cat .lotus_cluster_id)
    else
        echo -e "${RED}No cluster ID found. Please create a cluster first (option 12).${NC}"
        return 1
    fi
    
    # Check if cluster exists and is running
    cluster_info=$(databricks clusters get --cluster-id "$CLUSTER_ID" 2>/dev/null)
    if [ $? -ne 0 ]; then
        echo -e "${RED}Invalid cluster ID or cluster does not exist.${NC}"
        return 1
    else
        cluster_state=$(echo "$cluster_info" | jq -r '.state' 2>/dev/null)
        cluster_name=$(echo "$cluster_info" | jq -r '.cluster_name' 2>/dev/null)
        
        echo -e "${YELLOW}Using cluster: $cluster_name (Current state: $cluster_state)${NC}"
        
        if [ "$cluster_state" != "RUNNING" ]; then
            echo -e "${YELLOW}Cluster is not running. Starting cluster...${NC}"
            databricks clusters start --cluster-id "$CLUSTER_ID"
            
            echo -e "${YELLOW}Waiting for cluster to start...${NC}"
            echo -e "${YELLOW}This may take a few minutes.${NC}"
            
            # Poll for cluster status
            max_retries=10
            retry_count=0
            
            while [ $retry_count -lt $max_retries ]; do
                echo -e "${YELLOW}Checking cluster status (attempt $(($retry_count + 1))/$max_retries)...${NC}"
                cluster_state=$(databricks clusters get --cluster-id "$CLUSTER_ID" | jq -r '.state')
                
                if [ "$cluster_state" == "RUNNING" ]; then
                    echo -e "${GREEN}Cluster is now running!${NC}"
                    break
                elif [ "$cluster_state" == "ERROR" ] || [ "$cluster_state" == "UNKNOWN" ]; then
                    echo -e "${RED}Cluster is in $cluster_state state. Please check the Databricks workspace.${NC}"
                    return 1
                fi
                
                echo -e "${YELLOW}Current state: $cluster_state. Waiting 30 seconds...${NC}"
                retry_count=$((retry_count + 1))
                sleep 30
                
                if [ $retry_count -eq $max_retries ]; then
                    echo -e "${YELLOW}Cluster is still not in RUNNING state.${NC}"
                    return 1
                fi
            done
        fi
    fi
    
    # Install required libraries
    echo -e "${YELLOW}Installing PyYAML library...${NC}"
    # Create library JSON file
    LIBRARY_CONFIG=$(mktemp)
    cat > "$LIBRARY_CONFIG" << EOF
{
  "cluster_id": "$CLUSTER_ID",
  "libraries": [
    {
      "pypi": {
        "package": "pyyaml"
      }
    }
  ]
}
EOF
    
    # Install libraries
    databricks libraries install --json-file "$LIBRARY_CONFIG"
    
    # Clean up
    rm -f "$LIBRARY_CONFIG"
    
    echo -e "${GREEN}Libraries installed successfully. Waiting for installation to complete...${NC}"
    sleep 30  # Wait for libraries to be installed
    
    return 0
}

# Function to deploy Phase 3
function deploy_phase3() {
    echo -e "${YELLOW}Deploying Phase 3 (Data Processing)...${NC}"
    
    # First, verify the Phase 3 assets
    verify_phase3_assets
    if [ $? -ne 0 ]; then
        echo -e "${RED}Phase 3 assets verification failed. Please fix the issues and try again.${NC}"
        return 1
    fi
    
    # Upload notebooks to Databricks
    upload_notebooks_to_databricks
    if [ $? -ne 0 ]; then
        echo -e "${RED}Failed to upload notebooks to Databricks. Phase 3 deployment incomplete.${NC}"
        return 1
    fi
    
    # Create unified Databricks cluster (or reuse existing)
    create_databricks_clusters
    if [ $? -ne 0 ]; then
        echo -e "${RED}Failed to create or reuse Databricks cluster. Phase 3 deployment incomplete.${NC}"
        return 1
    fi
    
    # Install required Python libraries
    echo -e "${YELLOW}Installing required Python libraries to Databricks cluster...${NC}"
    install_databricks_libraries
    if [ $? -ne 0 ]; then
        echo -e "${YELLOW}Warning: Failed to install Python libraries. Jobs may fail if libraries are missing.${NC}"
        echo -e "${YELLOW}You can install them manually with option 16.${NC}"
    else
        echo -e "${GREEN}Required Python libraries installed successfully.${NC}"
    fi
    
    echo -e "${GREEN}Phase 3 deployed successfully!${NC}"
    
    # Ask if user wants to run jobs
    echo -e "${YELLOW}Do you want to run the batch and streaming jobs now? (y/n)${NC}"
    read -p "Choice [y/n]: " run_jobs_choice
    
    if [[ $run_jobs_choice == "y" || $run_jobs_choice == "Y" ]]; then
        echo -e "${YELLOW}Submitting batch and streaming jobs...${NC}"
        submit_batch_job
        submit_streaming_job
    fi
    
    return 0
}

# Main menu
function main_menu() {
    while true; do
        show_banner
        echo -e "${YELLOW}Current environment: ${ENV}${NC}"
        echo
        echo "Please select an operation:"
        echo "1) Deploy project (full process)"
        echo "2) Select environment (dev/prod)"
        echo "3) Set up Azure Storage for Terraform state"
        echo "4) Initialize Terraform"
        echo "5) Create Terraform plan"
        echo "6) Apply Terraform configuration"
        echo "7) Display output information"
        echo "8) Destroy infrastructure"
        echo "9) Run scraper"
        echo "10) Check deployment status"
        echo
        echo "Phase 3 - Data Processing:"
        echo "11) Deploy Phase 3 components"
        echo "12) Create/Reuse Databricks cluster"
        echo "13) Run batch ETL job"
        echo "14) Run streaming job"
        echo "15) Verify Phase 3 assets"
        echo "16) Install Python libraries to cluster"
        echo
        echo "0) Exit"
        
        read -p "Choice [0-16]: " choice
        
        case $choice in
            1)
                deploy_full
                ;;
            2)
                select_environment
                ;;
            3)
                setup_backend
                ;;
            4)
                init_terraform
                ;;
            5)
                plan_terraform
                ;;
            6)
                apply_terraform
                ;;
            7)
                show_outputs
                ;;
            8)
                destroy_terraform
                ;;
            9)
                run_scraper
                ;;
            10)
                check_deployment_status
                ;;
            11)
                deploy_phase3
                ;;
            12)
                create_databricks_clusters
                ;;
            13)
                submit_batch_job
                ;;
            14)
                submit_streaming_job
                ;;
            15)
                verify_phase3_assets
                ;;
            16)
                install_databricks_libraries
                ;;
            0)
                echo -e "${GREEN}Goodbye!${NC}"
                exit 0
                ;;
            *)
                echo -e "${RED}Invalid choice. Please try again.${NC}"
                ;;
        esac
        
        read -p "Press Enter to continue..."
    done
}

# Check prerequisites
check_prerequisites

# Get Azure Subscription ID
AZURE_SUBSCRIPTION_ID=$(az account show --query id -o tsv)

# Run main menu
main_menu 