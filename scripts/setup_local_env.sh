#!/usr/bin/env bash
# setup_local_env.sh — Local environment setup for Contoso Fabric Platform
set -euo pipefail

echo "Setting up Contoso Fabric Platform local environment..."

# Create virtual environment
if [ ! -d ".venv" ]; then
    echo "Creating virtual environment..."
    python3 -m venv .venv
fi

# Activate venv
source .venv/bin/activate

# Upgrade pip
pip install --upgrade pip --quiet

# Install dependencies
echo "Installing Python dependencies..."
pip install -r requirements.txt --quiet

echo "Dependencies installed successfully."

# Create lakehouse directory structure
echo "Creating lakehouse directory structure..."
mkdir -p lakehouse/bronze/raw/pos_transactions
mkdir -p lakehouse/bronze/raw/inventory_movements
mkdir -p lakehouse/bronze/raw/customers
mkdir -p lakehouse/bronze/raw/clickstream
mkdir -p lakehouse/bronze/pos_transactions
mkdir -p lakehouse/bronze/inventory_movements
mkdir -p lakehouse/bronze/customers
mkdir -p lakehouse/bronze/clickstream
mkdir -p lakehouse/silver/pos_transactions
mkdir -p lakehouse/silver/inventory_movements
mkdir -p lakehouse/silver/customers
mkdir -p lakehouse/silver/clickstream
mkdir -p lakehouse/gold/dim_customer
mkdir -p lakehouse/gold/dim_product
mkdir -p lakehouse/gold/dim_store
mkdir -p lakehouse/gold/dim_date
mkdir -p lakehouse/gold/fact_sales
mkdir -p lakehouse/gold/fact_inventory_snapshot
mkdir -p lakehouse/gold/agg_customer_360

echo "Lakehouse directories created."

# Create .agent-logs directory for hook logging
mkdir -p .agent-logs

# Create .env template if it doesn't exist
if [ ! -f ".env" ]; then
    echo "Creating .env template..."
    cat > .env << 'ENV_EOF'
# Contoso Fabric Platform — Environment Variables
# Copy this file and fill in your values. NEVER commit a filled-in .env file.

# Spark
SPARK_DRIVER_MEMORY=2g
SPARK_EXECUTOR_MEMORY=2g
LOG_LEVEL=INFO

# Source Connections (set actual values)
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=contoso_erp

# SFTP
SFTP_INVENTORY_HOST=sftp.contoso.com

# Weather API
WEATHER_API_BASE_URL=https://api.openweathermap.org

# Notifications
SMTP_HOST=smtp.contoso.com
SMTP_PORT=587
DQ_ALERT_EMAILS=data-team@contoso.com

# Microsoft Fabric (for deployment)
FABRIC_WORKSPACE_ID=
FABRIC_CLIENT_ID=
FABRIC_CLIENT_SECRET=
FABRIC_TENANT_ID=
ENV_EOF
    echo ".env template created. Fill in your values before running."
fi

echo ""
echo "Setup complete! Run the following to get started:"
echo "  source .venv/bin/activate"
echo "  make seed        # Generate sample data"
echo "  make test        # Run tests"
echo "  make run-bronze  # Run Bronze ingestion"
