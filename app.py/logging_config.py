"""
Logging Configuration
======================
Sets up file-based logging for the entire application.

Two layers:
  1. Global application log  → logs/app.log  (rotating, 10 MB × 5 backups)
  2. Per-document log         → storage/{user_id}/{doc_id}/processing.log

Call `setup_logging()` once at app startup (in main.py).
Call `get_document_logger()` in document_processor.py to write per-doc logs.
"""

import logging
import os
from logging.handlers import RotatingFileHandler

from config import STORAGE_PATH

# Global log directory at project root (sibling of app.py/)
_APP_DIR = os.path.dirname(os.path.abspath(__file__))
LOG_DIR = os.path.join(os.path.dirname(_APP_DIR), "logs")

_LOG_FORMAT = (
    "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"
)
_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"


def setup_logging(level: int = logging.DEBUG) -> None:
    """Configure root logger with console + rotating file handler."""
    os.makedirs(LOG_DIR, exist_ok=True)

    root = logging.getLogger()
    root.setLevel(level)

    formatter = logging.Formatter(_LOG_FORMAT, datefmt=_DATE_FORMAT)

    # ── Rotating file handler (global app log) ────────────────────────────
    file_handler = RotatingFileHandler(
        os.path.join(LOG_DIR, "app.log"),
        maxBytes=10 * 1024 * 1024,  # 10 MB
        backupCount=5,
        encoding="utf-8",
    )
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)
    root.addHandler(file_handler)

    # ── Console handler (keep existing uvicorn output intact) ─────────────
    if not any(isinstance(h, logging.StreamHandler) and not isinstance(h, RotatingFileHandler) for h in root.handlers):
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        console_handler.setFormatter(formatter)
        root.addHandler(console_handler)

    # ── Silence noisy third-party loggers ─────────────────────────────────
    for noisy in ("pdfminer", "pdfplumber", "urllib3", "httpcore", "httpx",
                  "google", "PIL", "multipart"):
        logging.getLogger(noisy).setLevel(logging.WARNING)


def get_document_log_path(user_id: int, document_id: str) -> str:
    """Return the path to a per-document processing log file."""
    doc_dir = os.path.join(STORAGE_PATH, str(user_id), document_id)
    os.makedirs(doc_dir, exist_ok=True)
    return os.path.join(doc_dir, "processing.log")


def get_document_logger(
    user_id: int,
    document_id: str,
) -> tuple:
    """
    Create a FileHandler that writes to the document's own processing.log.

    Returns (handler, log_path) so the caller can remove the handler when done.
    The handler is attached to the ROOT logger so ALL modules' log output
    (page_classifier, vlm_classifier, page_processor, etc.) is captured.
    """
    log_path = get_document_log_path(user_id, document_id)

    handler = logging.FileHandler(log_path, mode="w", encoding="utf-8")
    handler.setLevel(logging.DEBUG)
    handler.setFormatter(logging.Formatter(_LOG_FORMAT, datefmt=_DATE_FORMAT))

    logging.getLogger().addHandler(handler)
    return handler, log_path
