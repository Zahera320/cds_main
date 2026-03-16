"""
Database Migration — Add VLM verification columns
===================================================
Adds 'vlm_page_type', 'vlm_confidence', and 'vlm_agrees' columns
to the document_pages table.

Run once against an existing database:

    python migrate_add_vlm_columns.py

For fresh deployments this is unnecessary — create_all() builds the
columns from the updated model automatically.
"""

import sys
import os

# Ensure the app.py package is importable
sys.path.insert(0, os.path.dirname(__file__))

from sqlalchemy import text, inspect
from database import engine


def migrate():
    """Add VLM columns if they don't already exist."""
    inspector = inspect(engine)
    existing = {col["name"] for col in inspector.get_columns("document_pages")}

    stmts = []
    if "vlm_page_type" not in existing:
        stmts.append(
            "ALTER TABLE document_pages ADD COLUMN vlm_page_type VARCHAR"
        )
    if "vlm_confidence" not in existing:
        stmts.append(
            "ALTER TABLE document_pages ADD COLUMN vlm_confidence VARCHAR"
        )
    if "vlm_agrees" not in existing:
        stmts.append(
            "ALTER TABLE document_pages ADD COLUMN vlm_agrees BOOLEAN"
        )

    if not stmts:
        print("Nothing to migrate — VLM columns already exist.")
        return

    with engine.begin() as conn:
        for sql in stmts:
            print(f"  Running: {sql}")
            conn.execute(text(sql))

    print("VLM migration complete.")


if __name__ == "__main__":
    migrate()
