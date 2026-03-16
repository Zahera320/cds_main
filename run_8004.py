#!/usr/bin/env python3
"""
Run script for the FastAPI application.
Adds the app.py package directory to sys.path so all modules
are importable as top-level names.
"""
import sys
import os

# Re-execute with the project's venv Python if the venv's site-packages are
# not already active.  This ensures packages installed in the venv (docling,
# etc.) are available even when the script is invoked with the system python3.
_venv_python = "/home/ubuntu/env/bin/python3"
_venv_site = "/home/ubuntu/env/lib"
if os.path.exists(_venv_python) and not any(_venv_site in p for p in sys.path):
    os.execv(_venv_python, [_venv_python] + sys.argv)

import uvicorn

if __name__ == "__main__":
    project_dir = os.path.dirname(os.path.abspath(__file__))
    # app.py/ holds: main, auth, config, models, schemas, utils, database, logging_config
    sys.path.insert(0, os.path.join(project_dir, "app.py"))
    # project root holds: routes/, processing/, services/
    sys.path.insert(0, project_dir)
    uvicorn.run("main:app", host="0.0.0.0", port=8004, reload=False)