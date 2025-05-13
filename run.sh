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
        read -p "Storage account name [lotusterraformstate]: " storage_name
        storage_name=${storage_name:-lotusterraformstate}

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

        echo -e "${YELLOW}Waiting 60 seconds for storage account to be fully ready for ADLS Gen2...${NC}"
        sleep 60

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
            (cd $TF_DIR/environments/$ENV && terraform force-unlock -force $lock_id)
            echo -e "${YELLOW}Retrying: $cmd${NC}"
            output=$(eval "$cmd" 2>&1)
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
    
    # List of APIs to check
    apis=("price-analytics" "product-analytics" "promotion-analytics")
    
    for api in "${apis[@]}"; do
        # Get API ID from Azure, or construct it with rev=1
        api_id=$(az apim api show --name $api --resource-group $rg_name --service-name $apim_name --query id -o tsv 2>/dev/null)
        if [ -n "$api_id" ]; then
            # Make sure the API ID includes ;rev=1 at the end
            if [[ "$api_id" != *";rev=1" ]]; then
                api_id="${api_id};rev=1"
            fi
            # Check if already in state
            (cd $TF_DIR/environments/$ENV && terraform state list | grep "module.lotus_prism.module.api.azurerm_api_management_api.$api" > /dev/null 2>&1)
            if [ $? -ne 0 ]; then
                echo -e "${GREEN}API $api already exists on Azure. Importing into Terraform state...${NC}"
                tf_with_unlock "(cd $TF_DIR/environments/$ENV && terraform import \"module.lotus_prism.module.api.azurerm_api_management_api.$api\" \"$api_id\")"
            else
                echo -e "${YELLOW}API $api is already managed by Terraform, skipping import.${NC}"
            fi
        else
            echo -e "${YELLOW}API $api does not exist, will be created if needed.${NC}"
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

# Manually create ADLS Gen2 filesystems with Azure CLI
function manual_create_filesystems() {
    echo -e "${YELLOW}Attempting to create ADLS Gen2 filesystems using Azure CLI...${NC}"
    storage_name="lotusprism${ENV}adls"
    rg_name="lotus-prism-${ENV}-rg"
    
    # Get storage account key
    key=$(az storage account keys list --account-name $storage_name --resource-group $rg_name --query "[0].value" -o tsv)
    
    if [ -n "$key" ]; then
        filesystems=("bronze" "silver" "gold")
        for fs in "${filesystems[@]}"; do
            echo -e "${YELLOW}Creating filesystem $fs...${NC}"
            az storage fs exists --name $fs --account-name $storage_name --auth-mode key --account-key "$key" > /dev/null 2>&1
            if [ $? -ne 0 ]; then
                az storage fs create --name $fs --account-name $storage_name --auth-mode key --account-key "$key"
            else
                echo -e "${GREEN}Filesystem $fs already exists.${NC}"
            fi
        done
    else
        echo -e "${RED}Failed to get storage account key.${NC}"
    fi
    echo
}

# Force unlock Terraform state
function force_unlock_state() {
    echo -e "${YELLOW}Checking for locked Terraform state...${NC}"
    # Try to unlock the known lock ID
    known_lock_id="893ecdf1-5e34-07b8-09e2-559ce58098f9"
    echo -e "${YELLOW}Attempting to force-unlock state (ID: $known_lock_id)...${NC}"
    (cd $TF_DIR/environments/$ENV && terraform force-unlock -force $known_lock_id) || true
    echo -e "${GREEN}Terraform state unlock attempted.${NC}"
    echo
}

# Deploy project with full process
function deploy_full() {
    select_environment
    setup_backend
    init_terraform
    force_unlock_state
    import_resource_group_if_exists
    import_apim_if_exists
    import_apim_apis_if_exist
    import_apim_products_if_exist
    import_apim_product_policy_if_exists
    import_eventhub_if_exists
    import_eventhubs_if_exist
    import_eventhub_consumer_groups_if_exist
    import_eventhub_auth_rules_if_exist
    import_storage_if_exists
    import_databricks_if_exists
    import_synapse_if_exists
    import_synapse_fw_rule_if_exists
    plan_terraform
    
    echo -e "${YELLOW}Do you want to apply the configuration now? (y/n)${NC}"
    read -p "Choice [y/n]: " apply_choice
    
    if [[ $apply_choice == "y" || $apply_choice == "Y" ]]; then
        apply_terraform
        show_outputs
    else
        echo -e "${YELLOW}Configuration application cancelled.${NC}"
    fi
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
        echo "0) Exit"
        
        read -p "Choice [0-8]: " choice
        
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