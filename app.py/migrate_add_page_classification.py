"""
Database Migration — Add page classification columns
=====================================================
Adds 'page_type' and 'is_relevant' columns to the document_pages table.

Run once against an existing database:

    python migrate_add_page_classification.py

For fresh deployments this is unnecessary — create_all() builds the
columns from the updated model automatically.
"""

import sys
import os

# Ensure the app.py package is importable
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "app.py"))

from sqlalchemy import text, inspect
from database import engine


def migrate():
    """Add page_type and is_relevant columns if they don't already exist."""
    inspector = inspect(engine)
    existing = {col["name"] for col in inspector.get_columns("document_pages")}

    stmts = []
    if "page_type" not in existing:
        stmts.append(
            "ALTER TABLE document_pages ADD COLUMN page_type VARCHAR"
        )
    if "is_relevant" not in existing:
        stmts.append(
            "ALTER TABLE document_pages ADD COLUMN is_relevant BOOLEAN"
        )

    if not stmts:
        print("Nothing to migrate — columns already exist.")
        return

    with engine.begin() as conn:
        for sql in stmts:
            print(f"  Running: {sql}")
            conn.execute(text(sql))

    print("Migration complete.")


if __name__ == "__main__":
    migrate()
