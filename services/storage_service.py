"""
Storage Service
================
File-system helpers for user document storage.

Responsibilities:
    - Build per-user / per-document directory paths
    - Create directories on disk
    - Save raw uploaded bytes to the correct location
    - Sanitize filenames for safe OS storage
    - Validate that a requested image path is inside STORAGE_PATH

No database logic lives here.
"""

import os
from typing import Optional

from config import STORAGE_PATH


class StorageService:
    """Stateless helpers — every method is a classmethod."""

    # ── Path builders ─────────────────────────────────────────────────────────

    @classmethod
    def original_dir(cls, user_id: int, document_id: str) -> str:
        """Return (and create) the directory for the raw uploaded file."""
        path = os.path.join(STORAGE_PATH, str(user_id), document_id, "original")
        os.makedirs(path, exist_ok=True)
        return path

    @classmethod
    def pages_dir(cls, user_id: int, document_id: str) -> str:
        """Return (and create) the directory for rendered page images."""
        path = os.path.join(STORAGE_PATH, str(user_id), document_id, "pages")
        os.makedirs(path, exist_ok=True)
        return path

    @classmethod
    def pipeline_dir(cls, user_id: int, document_id: str) -> str:
        """Return (and create) the directory for autocount pipeline outputs."""
        path = os.path.join(STORAGE_PATH, str(user_id), document_id, "pipeline")
        os.makedirs(path, exist_ok=True)
        return path

    @classmethod
    def batch_pipeline_dir(cls, user_id: int, batch_id: str) -> str:
        """Return (and create) the directory for batch-level pipeline outputs."""
        path = os.path.join(STORAGE_PATH, str(user_id), f"batch_{batch_id}", "pipeline")
        os.makedirs(path, exist_ok=True)
        return path

    # ── File operations ───────────────────────────────────────────────────────

    @classmethod
    def sanitize_filename(cls, filename: str, max_name_len: int = 100) -> str:
        """
        Remove dangerous characters then truncate the stem to *max_name_len*
        chars while keeping the extension.

        Without stripping path separators and null bytes an attacker could
        supply a filename like  '../../etc/passwd'  or  'foo\x00.pdf'
        and write files outside the intended storage directory.
        """
        import re
        # Strip any directory components supplied by the client
        filename = os.path.basename(filename)
        # Remove null bytes and other control characters
        filename = filename.replace("\x00", "")
        name, ext = os.path.splitext(filename)
        # Keep only safe characters in the stem
        name = re.sub(r'[^\w\-. ]', '_', name).strip()
        if not name:
            name = "file"
        if len(name) > max_name_len:
            name = name[:max_name_len]
        return name + ext

    @classmethod
    def save_file(cls, directory: str, filename: str, contents: bytes) -> str:
        """Write *contents* to *directory/filename* and return the full path."""
        file_path = os.path.join(directory, filename)
        with open(file_path, "wb") as fh:
            fh.write(contents)
        return file_path

    @classmethod
    def cleanup_file(cls, path: str) -> None:
        """Best-effort removal (ignore errors)."""
        try:
            if os.path.exists(path):
                os.remove(path)
        except OSError:
            pass

    @classmethod
    def cleanup_document_dir(cls, user_id: int, document_id: str) -> None:
        """Best-effort recursive removal of the entire document directory."""
        import shutil
        doc_dir = os.path.join(STORAGE_PATH, str(user_id), document_id)
        try:
            if os.path.isdir(doc_dir):
                shutil.rmtree(doc_dir)
        except OSError:
            pass

    # ── Security ──────────────────────────────────────────────────────────────

    @classmethod
    def validate_image_path(cls, requested_path: str) -> Optional[str]:
        """
        Return the real absolute path if it is inside STORAGE_PATH **and**
        points to an existing file; otherwise return None.
        """
        abs_storage = os.path.realpath(STORAGE_PATH)
        abs_path = os.path.realpath(requested_path)
        if not abs_path.startswith(abs_storage + os.sep):
            return None
        if not os.path.isfile(abs_path):
            return None
        return abs_path

    @classmethod
    def validate_user_image_path(cls, requested_path: str, user_id: int) -> Optional[str]:
        """
        Like validate_image_path but also enforces that the path belongs
        to *user_id* (i.e. lives under storage/{user_id}/).

        Returns the real absolute path on success, None on any failure.
        """
        abs_user_storage = os.path.realpath(
            os.path.join(STORAGE_PATH, str(user_id))
        )
        abs_path = os.path.realpath(requested_path)
        if not abs_path.startswith(abs_user_storage + os.sep):
            return None
        if not os.path.isfile(abs_path):
            return None
        return abs_path

    @classmethod
    def locate_pdf(cls, user_id: int, document_id: str) -> str:
        """
        Find the first PDF inside the original/ directory.
        Raises FileNotFoundError if none exists.
        """
        original = os.path.join(STORAGE_PATH, str(user_id), document_id, "original")
        if not os.path.isdir(original):
            raise FileNotFoundError(f"Original directory not found: {original}")
        for fname in os.listdir(original):
            if fname.lower().endswith(".pdf"):
                return os.path.join(original, fname)
        raise FileNotFoundError(f"No PDF file found inside: {original}")

    @classmethod
    def locate_original_file(cls, user_id: int, document_id: str, file_type: str) -> str:
        """
        Find the original uploaded file (PDF or ZIP) inside the original/ directory.
        Raises FileNotFoundError if the directory or matching file does not exist.
        """
        original = os.path.join(STORAGE_PATH, str(user_id), document_id, "original")
        if not os.path.isdir(original):
            raise FileNotFoundError(f"Original directory not found: {original}")
        ext = ".pdf" if file_type == "pdf" else ".zip"
        for fname in os.listdir(original):
            if fname.lower().endswith(ext):
                return os.path.join(original, fname)
        raise FileNotFoundError(f"No {ext} file found inside: {original}")
