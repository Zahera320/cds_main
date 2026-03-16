"""
Migration: Add batch_id column to documents table.
Run once:  python3 -c "import sys; sys.path.insert(0,'app.py'); from migrate_add_batch_id import migrate; migrate()"
"""
import logging
from sqlalchemy import text
from database import engine

logger = logging.getLogger(__name__)


def migrate():
    """Add batch_id column to documents table if it doesn't exist."""
    with engine.connect() as conn:
        # Check if column exists
        result = conn.execute(text(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_name='documents' AND column_name='batch_id'"
        ))
        if result.fetchone():
            print("batch_id column already exists — skipping.")
            return

        conn.execute(text(
            "ALTER TABLE documents ADD COLUMN batch_id VARCHAR NULL"
        ))
        conn.execute(text(
            "CREATE INDEX ix_documents_batch_id ON documents (batch_id)"
        ))
        conn.commit()
        print("Added batch_id column and index to documents table.")


if __name__ == "__main__":
    migrate()
