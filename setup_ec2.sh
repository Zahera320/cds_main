#!/bin/bash
set -e

# Start PostgreSQL
sudo systemctl start postgresql
sudo systemctl enable postgresql

# Create database and user
sudo -u postgres psql -c "CREATE USER docuser WITH PASSWORD 'docpass123';" 2>/dev/null || echo "User already exists"
sudo -u postgres psql -c "CREATE DATABASE docupload OWNER docuser;" 2>/dev/null || echo "Database already exists"
sudo -u postgres psql -c "GRANT ALL PRIVILEGES ON DATABASE docupload TO docuser;" 2>/dev/null || echo "Privileges already granted"

# Create .env file
cat > /home/ubuntu/cds-main/project/.env << 'EOF'
DATABASE_URL=postgresql://docuser:docpass123@localhost:5432/docupload
SECRET_KEY=ec2-production-secret-key-change-me
ACCESS_TOKEN_EXPIRE_MINUTES=1440
MAX_FILE_SIZE_MB=100
STORAGE_PATH=/home/ubuntu/cds-main/project/storage
CORS_ORIGINS=*
EOF

# Create storage directory
mkdir -p /home/ubuntu/cds-main/project/storage

# Setup Python virtual environment
python3 -m venv /home/ubuntu/env
source /home/ubuntu/env/bin/activate
pip install --upgrade pip
pip install -r /home/ubuntu/cds-main/project/requirements.txt

echo "=== Setup complete ==="
echo "Database: docupload"
echo "User: docuser"
echo "Project: /home/ubuntu/cds-main/project"
echo "Venv: /home/ubuntu/env"
