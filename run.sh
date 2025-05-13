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
        
        # Input resource names
        read -p "Resource group name [terraform-state-rg]: " rg_name
        rg_name=${rg_name:-terraform-state-rg}
        
        read -p "Location [eastasia]: " location
        location=${location:-eastasia}
        
        read -p "Storage account name [lotusterraformstate]: " storage_name
        storage_name=${storage_name:-lotusterraformstate}
        
        read -p "Container name [tfstate]: " container_name
        container_name=${container_name:-tfstate}
        
        # Create resource group
        echo "Creating resource group..."
        az group create --name $rg_name --location $location
        
        # Create storage account
        echo "Creating storage account..."
        az storage account create --name $storage_name --resource-group $rg_name --sku Standard_LRS
        
        # Create container
        echo "Creating container..."
        az storage container create --name $container_name --account-name $storage_name
        
        # Get storage key
        echo "Getting storage key..."
        ACCOUNT_KEY=$(az storage account keys list --resource-group $rg_name --account-name $storage_name --query [0].value -o tsv)
        
        # Initialize Terraform with backend
        echo "Initializing Terraform with Azure backend..."
        (cd $TF_DIR/environments/$ENV && terraform init \
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
    (cd $TF_DIR/environments/$ENV && terraform plan -out=tfplan)
    echo -e "${GREEN}Terraform plan created successfully.${NC}"
    echo
}

# Apply Terraform
function apply_terraform() {
    echo -e "${YELLOW}Applying Terraform configuration...${NC}"
    
    # Check if plan file exists
    if [ -f "$TF_DIR/environments/$ENV/tfplan" ]; then
        (cd $TF_DIR/environments/$ENV && terraform apply tfplan)
    else
        (cd $TF_DIR/environments/$ENV && terraform apply)
    fi
    
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

# Deploy project with full process
function deploy_full() {
    select_environment
    setup_backend
    init_terraform
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

# Run main menu
main_menu 