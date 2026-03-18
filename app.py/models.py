from sqlalchemy import Column, Index, Integer, String, Boolean, ForeignKey, DateTime, UniqueConstraint, text
from sqlalchemy.orm import relationship
from datetime import datetime, timezone

from database import Base


def _utcnow():
    """Return current UTC time as a naive datetime (compatible with SQLAlchemy DateTime)."""
    return datetime.now(timezone.utc).replace(tzinfo=None)


class User(Base):
    __tablename__ = "users"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, nullable=False)
    email = Column(String, unique=True, index=True, nullable=False)
    hashed_password = Column(String, nullable=False)
    role = Column(String, nullable=False, default="user")
    created_at = Column(DateTime, default=_utcnow)

    documents = relationship("Document", back_populates="owner", cascade="all, delete-orphan")


class Document(Base):
    __tablename__ = "documents"

    id = Column(Integer, primary_key=True, index=True)
    document_id = Column(String, unique=True, index=True, nullable=False)  # UUID
    filename = Column(String, nullable=False)
    file_type = Column(String, nullable=False)  # 'pdf' or 'zip'
    file_size = Column(Integer, nullable=False)
    # SHA-256 hex digest of the raw uploaded bytes (64 chars).
    # Used for per-user deduplication: a user cannot upload the same
    # file content twice.  NULL is allowed so that rows migrated from
    # before this column existed are unaffected.
    content_hash = Column(String(64), nullable=True, index=True)
    owner_id = Column(Integer, ForeignKey("users.id"), nullable=False, index=True)
    upload_time = Column(DateTime, default=_utcnow)
    status = Column(String, default="uploaded", nullable=False)  # uploaded, processing, completed, failed
    # Batch grouping: all documents from the same multi-PDF or ZIP upload
    # share the same batch_id.  NULL means standalone single-PDF upload.
    batch_id = Column(String, nullable=True, index=True)
    page_count = Column(Integer, nullable=True)
    pages_processed = Column(Integer, nullable=True, default=0)
    processing_progress = Column(Integer, nullable=True, default=0)
    processing_started_at = Column(DateTime, nullable=True)
    processing_completed_at = Column(DateTime, nullable=True)
    processing_error = Column(String, nullable=True)
    classification_message = Column(String, nullable=True)

    owner = relationship("User", back_populates="documents")
    pages = relationship("DocumentPage", back_populates="document", cascade="all, delete-orphan")

    # Partial unique index: enforces one (owner, content_hash) pair while
    # still allowing multiple NULL hashes (old rows without a hash).
    __table_args__ = (
        Index(
            "uq_documents_owner_content_hash",
            "owner_id",
            "content_hash",
            unique=True,
            postgresql_where=text("content_hash IS NOT NULL"),
        ),
    )


class DocumentPage(Base):
    """Stores per-page data extracted from a PDF document."""
    __tablename__ = "document_pages"

    id = Column(Integer, primary_key=True, index=True)
    document_id = Column(String, ForeignKey("documents.document_id"), nullable=False, index=True)
    page_number = Column(Integer, nullable=False)
    extracted_text = Column(String, nullable=True)          # Full page text
    text_length = Column(Integer, nullable=False, default=0) # Character count
    ocr_used = Column(Boolean, nullable=False, default=False) # Whether OCR was applied
    image_path = Column(String, nullable=True)               # Path to 300-DPI PNG
    page_type = Column(String, nullable=True)                # Classified page type (e.g. LIGHTING_PLAN)
    is_relevant = Column(Boolean, nullable=True)             # True if relevant for fixture analysis
    has_fixture_schedule = Column(Boolean, nullable=True, default=False)  # True if page contains Light Fixture Schedule
    vlm_page_type = Column(String, nullable=True)            # VLM verification result
    vlm_confidence = Column(String, nullable=True)           # VLM confidence: high/medium/low
    vlm_agrees = Column(Boolean, nullable=True)              # True if VLM agrees with rule-based
    confidence_source = Column(String, nullable=True)        # Source of classification confidence
    sheet_code = Column(String, nullable=True)               # Sheet code extracted from page
    created_at = Column(DateTime, default=_utcnow)

    document = relationship("Document", back_populates="pages")

    # Prevent duplicate pages if a background task runs twice
    # (e.g. server crash + retry or accidental re-trigger).
    __table_args__ = (
        UniqueConstraint("document_id", "page_number", name="uq_docpage_docid_pagenum"),
    )