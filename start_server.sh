#!/bin/bash
cd /home/ubuntu/cds-main/project
source /home/ubuntu/env/bin/activate

# Initialize database tables
python3 -c "
import sys, os
project_dir = '/home/ubuntu/cds-main/project'
sys.path.insert(0, os.path.join(project_dir, 'app.py'))
sys.path.insert(0, project_dir)
from database import engine
from models import Base
Base.metadata.create_all(bind=engine)
print('Tables created successfully!')
"

# Start the server with nohup so it persists after SSH disconnect
nohup python3 run.py > /home/ubuntu/cds-main/project/server.log 2>&1 &
echo "Server PID: $!"
sleep 2
echo "=== Server log ==="
cat /home/ubuntu/cds-main/project/server.log
echo ""
echo "=== API should be live at http://localhost:1234 ==="
