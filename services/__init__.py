"""
Services Package
=================
Thin service layer that sits between routes and processing.
Keeps route handlers slim and processing modules DB-free.

Public API:
    StorageService  — file-system path helpers and file persistence
    ZipService      — ZIP extraction with path-traversal protection
    DocumentService — DB operations for documents and pages
"""
from services.storage_service import StorageService
from services.zip_service import ZipService
from services.document_service import DocumentService

__all__ = ["StorageService", "ZipService", "DocumentService"]
