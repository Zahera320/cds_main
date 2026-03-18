"""
Database Migration — Add has_fixture_schedule column
=====================================================
Adds 'has_fixture_schedule' column to the document_pages table
to track pages containing Light Fixture Schedule tables.

Run once against an existing database:

    python migrate_add_fixture_schedule_flag.py

For fresh deployments this is unnecessary — create_all() builds the
column from the updated model automatically.
"""

import sys
import os

# Ensure the app.py package is importable
sys.path.insert(0, os.path.dirname(__file__))

from sqlalchemy import text, inspect
from database import engine


def migrate():
    """Add has_fixture_schedule column if it doesn't already exist."""
    inspector = inspect(engine)
    existing = {col["name"] for col in inspector.get_columns("document_pages")}

    if "has_fixture_schedule" in existing:
        print("Nothing to migrate — has_fixture_schedule column already exists.")
        return

    stmt = "ALTER TABLE document_pages ADD COLUMN has_fixture_schedule BOOLEAN DEFAULT FALSE"

    with engine.begin() as conn:
        print(f"  Running: {stmt}")
        conn.execute(text(stmt))

    print("Migration complete — has_fixture_schedule column added.")


if __name__ == "__main__":
    migrate()
