"""
Document Service
=================
Database operations for documents and their pages.

Responsibilities:
    - Create / query / update Document rows
    - Create DocumentPage rows from processing results
    - Update document status lifecycle

This keeps all ORM `db.add / db.commit` calls in one place so that
the processing pipeline and route layer stay DB-free.
"""
from __future__ import annotations

import logging
from typing import Dict, Any, List, Optional

from sqlalchemy.orm import Session

import models

logger = logging.getLogger(__name__)


class DocumentService:
    """All methods take a SQLAlchemy Session as the first argument."""

    # ── Document CRUD ─────────────────────────────────────────────────────────

    @staticmethod
    def create_document(db: Session, **kwargs) -> models.Document:
        """Insert a new Document row and return the refreshed ORM object."""
        doc = models.Document(**kwargs)
        db.add(doc)
        db.commit()
        db.refresh(doc)
        return doc

    @staticmethod
    def get_document(db: Session, document_id: str) -> Optional[models.Document]:
        """Fetch by UUID (no owner filter — caller should add it)."""
        return (
            db.query(models.Document)
            .filter(models.Document.document_id == document_id)
            .first()
        )

    @staticmethod
    def find_duplicate(
        db: Session, owner_id: int, content_hash: str
    ) -> Optional[models.Document]:
        """
        Return the existing Document if this user already uploaded
        identical file content (matched by SHA-256 hash), else None.

        This is the primary deduplication guard: called *before* writing
        to disk or inserting a new DB row.
        """
        return (
            db.query(models.Document)
            .filter(
                models.Document.owner_id == owner_id,
                models.Document.content_hash == content_hash,
            )
            .first()
        )

    @staticmethod
    def get_user_document(
        db: Session, document_id: str, owner_id: int
    ) -> Optional[models.Document]:
        """Fetch by UUID scoped to a specific owner."""
        return (
            db.query(models.Document)
            .filter(
                models.Document.document_id == document_id,
                models.Document.owner_id == owner_id,
            )
            .first()
        )

    @staticmethod
    def list_user_documents(
        db: Session, owner_id: int, skip: int = 0, limit: int = 50
    ) -> List[models.Document]:
        """
        Return documents for *owner_id*, newest first, with pagination.

        Without pagination a user with thousands of documents would cause
        a massive query result loaded entirely into memory on every request.
        """
        return (
            db.query(models.Document)
            .filter(models.Document.owner_id == owner_id)
            .order_by(models.Document.upload_time.desc())
            .offset(skip)
            .limit(limit)
            .all()
        )

    @staticmethod
    def delete_document(db: Session, document: models.Document) -> None:
        """
        Delete a document and all its pages (cascade).
        The caller is responsible for cleaning up files on disk.
        """
        db.delete(document)
        db.commit()
        logger.info("Deleted document %s (owner %d)", document.document_id, document.owner_id)

    # ── Status lifecycle ──────────────────────────────────────────────────────

    @staticmethod
    def update_status(db: Session, document: models.Document, new_status: str) -> None:
        """
        Persist a status change.
        uploaded → processing → completed | failed
        """
        try:
            document.status = new_status
            db.commit()
            logger.info("Document %s → status: %s", document.document_id, new_status)
        except Exception as exc:
            db.rollback()
            logger.error(
                "Failed to update document %s status to '%s': %s",
                document.document_id,
                new_status,
                exc,
            )

    # ── Page persistence ──────────────────────────────────────────────────────

    @staticmethod
    def persist_page(db: Session, document_id: str, page_data: Dict[str, Any]) -> None:
        """
        Insert a single DocumentPage row from a processing result dict.

        Expected keys in *page_data*:
            page_number, extracted_text, text_length, ocr_used, image_path
        Optional keys (page classification):
            page_type, is_relevant, has_fixture_schedule, confidence_source, sheet_code
        """
        page = models.DocumentPage(
            document_id=document_id,
            page_number=page_data["page_number"],
            extracted_text=page_data["extracted_text"],
            text_length=page_data["text_length"],
            ocr_used=page_data["ocr_used"],
            image_path=page_data["image_path"],
            page_type=page_data.get("page_type"),
            is_relevant=page_data.get("is_relevant"),
            has_fixture_schedule=page_data.get("has_fixture_schedule", False),
            confidence_source=page_data.get("confidence_source"),
            sheet_code=page_data.get("sheet_code"),
            vlm_page_type=page_data.get("vlm_page_type"),
            vlm_confidence=page_data.get("vlm_confidence"),
            vlm_agrees=page_data.get("vlm_agrees"),
        )
        db.add(page)
        db.commit()

    @staticmethod
    def persist_pages_batch(
        db: Session, document_id: str, pages_data: List[Dict[str, Any]]
    ) -> None:
        """
        Bulk-insert all DocumentPage rows for a document in ONE transaction.

        Dramatically faster than one commit per page (avoids N round-trips).
        Pages are sorted by page_number before insertion.
        """
        if not pages_data:
            return

        sorted_pages = sorted(pages_data, key=lambda p: p["page_number"])
        for page_data in sorted_pages:
            page = models.DocumentPage(
                document_id=document_id,
                page_number=page_data["page_number"],
                extracted_text=page_data["extracted_text"],
                text_length=page_data["text_length"],
                ocr_used=page_data["ocr_used"],
                image_path=page_data["image_path"],
                page_type=page_data.get("page_type"),
                is_relevant=page_data.get("is_relevant"),
                has_fixture_schedule=page_data.get("has_fixture_schedule", False),
                confidence_source=page_data.get("confidence_source"),
                sheet_code=page_data.get("sheet_code"),
                vlm_page_type=page_data.get("vlm_page_type"),
                vlm_confidence=page_data.get("vlm_confidence"),
                vlm_agrees=page_data.get("vlm_agrees"),
            )
            db.add(page)

        try:
            db.commit()
            logger.info(
                "Batch-inserted %d pages for document %s", len(sorted_pages), document_id
            )
        except Exception as exc:
            db.rollback()
            logger.error(
                "Batch page insert failed for document %s: %s", document_id, exc
            )
            raise

    @staticmethod
    def delete_pages(db: Session, document_id: str) -> int:
        """Delete all pages for a document (used before reprocessing)."""
        count = db.query(models.DocumentPage).filter(
            models.DocumentPage.document_id == document_id
        ).delete()
        db.commit()
        logger.info("Deleted %d existing pages for document %s", count, document_id)
        return count

    @staticmethod
    def get_pages(
        db: Session, document_id: str, skip: int = 0, limit: int = 50,
        relevance: str | None = None,
    ) -> List[models.DocumentPage]:
        """Return pages for a document, ordered by page number, with pagination.

        Args:
            relevance: Optional filter — 'relevant', 'irrelevant', or None (all).
        """
        q = (
            db.query(models.DocumentPage)
            .filter(models.DocumentPage.document_id == document_id)
        )
        if relevance == "relevant":
            q = q.filter(models.DocumentPage.is_relevant == True)
        elif relevance == "irrelevant":
            q = q.filter(
                (models.DocumentPage.is_relevant == False)
                | (models.DocumentPage.is_relevant == None)
            )
        return (
            q.order_by(models.DocumentPage.page_number)
            .offset(skip)
            .limit(limit)
            .all()
        )

    @staticmethod
    def count_pages(
        db: Session, document_id: str, relevance: str | None = None,
    ) -> int:
        """Return total number of pages for a document, optionally filtered."""
        q = (
            db.query(models.DocumentPage)
            .filter(models.DocumentPage.document_id == document_id)
        )
        if relevance == "relevant":
            q = q.filter(models.DocumentPage.is_relevant == True)
        elif relevance == "irrelevant":
            q = q.filter(
                (models.DocumentPage.is_relevant == False)
                | (models.DocumentPage.is_relevant == None)
            )
        return q.count()

    @staticmethod
    def get_all_pages(
        db: Session, document_id: str,
    ) -> List[models.DocumentPage]:
        """Return ALL pages for a document (no pagination), for summary use."""
        return (
            db.query(models.DocumentPage)
            .filter(models.DocumentPage.document_id == document_id)
            .order_by(models.DocumentPage.page_number)
            .all()
        )

    @staticmethod
    def update_page_classifications(
        db: Session,
        document_id: str,
        classifications: List[Dict[str, Any]],
    ) -> None:
        """
        Update page_type / is_relevant / confidence_source / sheet_code
        for existing DocumentPage rows.

        Each item in *classifications* must have:
            page_number, page_type, is_relevant, confidence_source, sheet_code
        """
        if not classifications:
            return
        cls_map = {c["page_number"]: c for c in classifications}
        pages = (
            db.query(models.DocumentPage)
            .filter(models.DocumentPage.document_id == document_id)
            .all()
        )
        for page in pages:
            c = cls_map.get(page.page_number)
            if c:
                page.page_type = c.get("page_type")
                page.is_relevant = c.get("is_relevant")
                page.confidence_source = c.get("confidence_source")
                page.sheet_code = c.get("sheet_code")
        try:
            db.commit()
            logger.info(
                "Updated classifications for %d pages in document %s",
                len(pages), document_id,
            )
        except Exception as exc:
            db.rollback()
            logger.error(
                "Failed to update classifications for document %s: %s",
                document_id, exc,
            )
            raise

    # ── Batch queries ─────────────────────────────────────────────────────────

    @staticmethod
    def get_batch_documents(
        db: Session, batch_id: str, owner_id: int
    ) -> List[models.Document]:
        """Return all documents in a batch, owned by the given user."""
        return (
            db.query(models.Document)
            .filter(
                models.Document.batch_id == batch_id,
                models.Document.owner_id == owner_id,
            )
            .order_by(models.Document.upload_time)
            .all()
        )

    @staticmethod
    def all_batch_completed(
        db: Session, batch_id: str, owner_id: int
    ) -> bool:
        """True when every document in the batch has status 'completed'."""
        docs = (
            db.query(models.Document)
            .filter(
                models.Document.batch_id == batch_id,
                models.Document.owner_id == owner_id,
            )
            .all()
        )
        return bool(docs) and all(d.status == "completed" for d in docs)
