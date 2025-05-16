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
    echo -e "  Price & Retail Intelligence API Server ${NC}"
    echo
}

# Show banner
show_banner

# Check if virtual environment exists
if [ ! -d "venv" ]; then
    echo -e "${YELLOW}Creating virtual environment...${NC}"
    python3 -m venv venv
fi

# Activate virtual environment
echo -e "${YELLOW}Activating virtual environment...${NC}"
source venv/bin/activate

# Check if requirements are installed
if ! pip show flask > /dev/null 2>&1; then
    echo -e "${YELLOW}Installing API requirements...${NC}"
    pip install -r api/requirements.txt
else
    echo -e "${GREEN}Flask is already installed.${NC}"
fi

# Check if .env file exists
if [ ! -f ".env" ]; then
    echo -e "${YELLOW}Creating basic .env file...${NC}"
    cat > .env << EOF
API_SECRET_KEY=lotus-prism-secret-key
PORT=8080
DEBUG=False
RATE_LIMIT=100
EOF
    echo -e "${GREEN}Created basic .env file.${NC}"
else
    # Update .env file with API configurations if needed
    if ! grep -q "^API_SECRET_KEY=" ".env"; then
        echo -e "${YELLOW}Adding API_SECRET_KEY to .env file...${NC}"
        echo "API_SECRET_KEY=lotus-prism-secret-key" >> .env
    fi
    
    if ! grep -q "^PORT=" ".env"; then
        echo -e "${YELLOW}Adding PORT to .env file...${NC}"
        echo "PORT=8080" >> .env
    else
        # Cập nhật PORT thành 8080 nếu đã tồn tại
        sed -i '' 's/^PORT=.*/PORT=8080/g' .env
    fi
    
    if ! grep -q "^DEBUG=" ".env"; then
        echo -e "${YELLOW}Adding DEBUG to .env file...${NC}"
        echo "DEBUG=False" >> .env
    fi
    
    if ! grep -q "^RATE_LIMIT=" ".env"; then
        echo -e "${YELLOW}Adding RATE_LIMIT to .env file...${NC}"
        echo "RATE_LIMIT=100" >> .env
    fi
fi

# Run the API server
PORT=$(grep "^PORT=" ".env" | cut -d'=' -f2)
DEBUG=$(grep "^DEBUG=" ".env" | cut -d'=' -f2)
echo -e "${GREEN}Starting API server at http://localhost:${PORT}${NC}"
echo -e "${YELLOW}Press Ctrl+C to stop the server${NC}"

# Go to the API directory and run the server with environment variables
cd api && python app.py

# This will only execute if the server is stopped
echo -e "${GREEN}API server stopped.${NC}"

# Deactivate virtual environment
deactivate 