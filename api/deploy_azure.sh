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
    echo -e "  Price & Retail Intelligence API - Azure Deployment ${NC}"
    echo
}

# Show banner
show_banner

# Check if az is installed
if ! command -v az &> /dev/null; then
    echo -e "${RED}Azure CLI is not installed. Please install it first.${NC}"
    exit 1
fi

# Check if logged in to Azure
echo -e "${YELLOW}Checking Azure login status...${NC}"
az account show &> /dev/null
if [ $? -ne 0 ]; then
    echo -e "${YELLOW}You are not logged into Azure. Initiating login...${NC}"
    az login
fi

# Ask for Azure app name
read -p "Enter a unique name for your Azure Web App [lotus-prism-api]: " app_name
app_name=${app_name:-lotus-prism-api}

# Ask for resource group
read -p "Enter resource group name [lotus-prism-rg]: " resource_group
resource_group=${resource_group:-lotus-prism-rg}

# Ask for location
read -p "Enter location [eastasia]: " location
location=${location:-eastasia}

# Check if resource group exists
echo -e "${YELLOW}Checking if resource group exists...${NC}"
az group show --name $resource_group &> /dev/null
if [ $? -ne 0 ]; then
    echo -e "${YELLOW}Resource group does not exist. Creating...${NC}"
    az group create --name $resource_group --location $location
    echo -e "${GREEN}Resource group created.${NC}"
else
    echo -e "${GREEN}Resource group already exists.${NC}"
fi

# Check if gunicorn is installed in requirements.txt
if ! grep -q "gunicorn" "requirements.txt"; then
    echo -e "${YELLOW}Adding gunicorn to requirements.txt...${NC}"
    echo "gunicorn>=20.1.0" >> requirements.txt
fi

# Create startup.txt file
echo -e "${YELLOW}Creating startup command file...${NC}"
echo "gunicorn --bind=0.0.0.0 --timeout 600 app:app" > startup.txt
echo -e "${GREEN}Startup command file created.${NC}"

# Create App Service Plan
echo -e "${YELLOW}Creating App Service Plan...${NC}"
az appservice plan create --name "${app_name}-plan" \
                         --resource-group $resource_group \
                         --location $location \
                         --sku B1 \
                         --is-linux

# Create Web App
echo -e "${YELLOW}Creating Web App...${NC}"
az webapp create --name $app_name \
                --resource-group $resource_group \
                --plan "${app_name}-plan" \
                --runtime "PYTHON:3.9"

# Configure Web App settings
echo -e "${YELLOW}Configuring Web App settings...${NC}"
az webapp config set --name $app_name \
                    --resource-group $resource_group \
                    --startup-file "startup.txt"

# Set environment variables
echo -e "${YELLOW}Setting environment variables...${NC}"
az webapp config appsettings set --name $app_name \
                                --resource-group $resource_group \
                                --settings \
                                API_SECRET_KEY="lotus-prism-secret-key" \
                                DEBUG="False" \
                                RATE_LIMIT="100"

# Deploy code to Web App
echo -e "${YELLOW}Deploying code to Web App...${NC}"
# Create a temporary deployment zip file
echo -e "${YELLOW}Creating deployment package...${NC}"
mkdir -p deploy
cp -r *.py requirements.txt startup.txt deploy/
cd deploy
zip -r ../deploy.zip .
cd ..
rm -rf deploy

# Deploy the zip file
echo -e "${YELLOW}Deploying zip file to Azure...${NC}"
az webapp deployment source config-zip --name $app_name \
                                       --resource-group $resource_group \
                                       --src deploy.zip

# Clean up
rm -f deploy.zip

# Show deployment info
echo -e "${GREEN}Deployment complete! API is now available at:${NC}"
echo -e "${BLUE}https://${app_name}.azurewebsites.net/api/health${NC}"
echo
echo -e "${YELLOW}To test the API, run:${NC}"
echo -e "curl https://${app_name}.azurewebsites.net/api/health"
echo
echo -e "${YELLOW}To check logs, run:${NC}"
echo -e "az webapp log tail --name ${app_name} --resource-group ${resource_group}" 