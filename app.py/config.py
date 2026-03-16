"""
Centralized configuration loaded from .env file.
All settings in one place — easy to change for production.
"""
import os
from dotenv import load_dotenv

# Load .env file from project root
load_dotenv(os.path.join(os.path.dirname(__file__), '..', '.env'))

# Database
DATABASE_URL: str = os.getenv("DATABASE_URL", "postgresql://postgres:postgres123@localhost:5432/docupload")

# JWT / Auth
SECRET_KEY: str = os.getenv("SECRET_KEY", "supersecret-change-me-in-production")
ALGORITHM: str = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES: int = int(os.getenv("ACCESS_TOKEN_EXPIRE_MINUTES", "1440"))

# File Upload
MAX_FILE_SIZE_MB: int = int(os.getenv("MAX_FILE_SIZE_MB", "100"))
MAX_FILE_SIZE: int = MAX_FILE_SIZE_MB * 1024 * 1024  # Convert to bytes
# Resolve to absolute relative to *this file's* directory so it is
# stable regardless of working-directory changes at runtime.
_config_dir = os.path.dirname(os.path.abspath(__file__))
STORAGE_PATH: str = os.path.abspath(
    os.path.join(_config_dir, os.getenv("STORAGE_PATH", "storage"))
)
ALLOWED_EXTENSIONS: set = {".pdf", ".zip"}

# Text Extraction & OCR
BASE_OCR_THRESHOLD: int = int(os.getenv("BASE_OCR_THRESHOLD", "30"))
MIN_TEXT_DENSITY: float = float(os.getenv("MIN_TEXT_DENSITY", "2.0"))  # chars per square inch
MAX_GARBLED_RATIO: float = float(os.getenv("MAX_GARBLED_RATIO", "0.3"))  # max ratio of garbled chars
OCR_ENHANCED_MODE: bool = os.getenv("OCR_ENHANCED_MODE", "true").lower() == "true"

# Parallel Processing
MAX_WORKERS: int = int(os.getenv("MAX_WORKERS", "4"))  # threads for page processing
CLASSIFICATION_WORKERS: int = int(os.getenv("CLASSIFICATION_WORKERS", "4"))  # threads for page classification
VLM_MAX_CONCURRENT: int = int(os.getenv("VLM_MAX_CONCURRENT", "3"))  # concurrent VLM API calls

# ──────────────────────────────────────────────────────────────────────────────
# DB_POOL_SIZE + MAX_WORKERS — must be tuned together
# ──────────────────────────────────────────────────────────────────────────────
# Every active request AND every background task holds exactly ONE database
# connection from the pool for its entire lifetime.  If you run out of pool
# connections, new requests block (and eventually time out) until a connection
# is returned.
#
# The safe rule is:
#
#   max_concurrent_connections ≤ pool_size + max_overflow
#   max_concurrent_connections = MAX_CONCURRENT_REQUESTS + MAX_WORKERS
#
# Example with defaults (pool_size=10, max_overflow=20, MAX_WORKERS=4):
#   Pool capacity = 10 + 20 = 30 connections.
#   Each background task uses 1 connection for its entire run.
#   Each HTTP request uses 1 connection while it is being handled.
#   If 4 background tasks are running continuously AND 26 requests arrive
#   simultaneously, the 31st connection attempt blocks → request queues.
#
# Recommended formula for tuning:
#   1. Check PostgreSQL: 'SHOW max_connections;'  (default is often 100)
#   2. Reserve ~10 connections for admin/monitoring tools.
#   3. Divide the rest by the number of app server replicas.
#   4. Set pool_size  ≈  reserved_per_replica * 0.5
#      Set max_overflow ≈ reserved_per_replica * 1.0
#      Set MAX_WORKERS  ≤  pool_size  (guarantee tasks always get a slot)
#
# Example — single replica, Postgres max_connections=100:
#   Available = 100 - 10 = 90
#   pool_size=20, max_overflow=40, MAX_WORKERS=10   → 60 connections max used
#
# Example — 4 replicas, Postgres max_connections=200:
#   Per replica = (200-10)/4 = 47
#   pool_size=10, max_overflow=20, MAX_WORKERS=8    → 38 connections max per replica
# ──────────────────────────────────────────────────────────────────────────────
AWS_ACCESS_KEY_ID: str = os.getenv("AWS_ACCESS_KEY_ID", "")
AWS_SECRET_ACCESS_KEY: str = os.getenv("AWS_SECRET_ACCESS_KEY", "")
AWS_REGION_NAME: str = os.getenv("AWS_REGION_NAME", "us-east-1")
S3_BUCKET_NAME: str = os.getenv("S3_BUCKET_NAME", "dhatri-cds")
# Database connection pool
DB_POOL_SIZE: int = int(os.getenv("DB_POOL_SIZE", "10"))
DB_MAX_OVERFLOW: int = int(os.getenv("DB_MAX_OVERFLOW", "20"))
DB_POOL_TIMEOUT: int = int(os.getenv("DB_POOL_TIMEOUT", "30"))

# VLM (Vision Language Model) — page classification verification
# Set GOOGLE_API_KEY to enable Gemini-based verification.
# When VLM_VERIFY is false (or key is missing), classification is rule-based only.
VLM_VERIFY: bool = os.getenv("VLM_VERIFY", "true").lower() == "true"
GOOGLE_API_KEY: str = os.getenv("GOOGLE_API_KEY", "")

# Anthropic API (used by table_extractor VLM fixture extraction)
ANTHROPIC_API_KEY: str = os.getenv("ANTHROPIC_API_KEY", "")

# CORS
CORS_ORIGINS: list = os.getenv("CORS_ORIGINS", "*").split(",")

