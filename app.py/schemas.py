from pydantic import BaseModel, EmailStr, Field, field_validator, ConfigDict
from datetime import datetime
from typing import Optional, List

# User schemas
class UserRegister(BaseModel):
    name: str
    email: EmailStr
    password: str

    @field_validator('name')
    @classmethod
    def validate_name(cls, v):
        if not v or len(v.strip()) < 2:
            raise ValueError('Name must be at least 2 characters long')
        return v.strip()

    @field_validator('password')
    @classmethod
    def validate_password(cls, v):
        if len(v) < 8:
            raise ValueError('Password must be at least 8 characters long')
        if not any(c.isupper() for c in v):
            raise ValueError('Password must contain at least one uppercase letter')
        if not any(c.islower() for c in v):
            raise ValueError('Password must contain at least one lowercase letter')
        if not any(c.isdigit() for c in v):
            raise ValueError('Password must contain at least one digit')
        return v

class UserLogin(BaseModel):
    email: EmailStr
    password: str

class UserResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    name: str
    email: str
    created_at: datetime

class Token(BaseModel):
    access_token: str
    token_type: str = "bearer"
    user: UserResponse

# Document schemas
class DocumentResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    document_id: str
    filename: str
    file_type: str
    file_size: int
    upload_time: datetime
    status: str
    content_hash: Optional[str] = None  # SHA-256 of raw file bytes; None for legacy rows
    batch_id: Optional[str] = None      # Shared UUID linking multi-PDF / ZIP uploads

class UploadResponse(BaseModel):
    document_id: str
    filename: str
    file_type: str
    file_size: int
    status: str
    batch_id: Optional[str] = None  # set when part of a multi-PDF or ZIP batch
    extracted_pdf_count: int = 0    # > 0 when a ZIP was uploaded
    duplicate: bool = False         # True when the server rejected a re-upload
    message: str


class BatchUploadResponse(BaseModel):
    """Response for uploading multiple files at once."""
    batch_id: str                   # shared batch UUID for all uploaded docs
    total: int
    succeeded: int
    failed: int
    results: List[UploadResponse]
    errors: List[dict] = []


class DeleteResponse(BaseModel):
    """Response for deleting a document."""
    document_id: str
    message: str


# ── Page schemas ──────────────────────────────────────────────────────────────

class DocumentPageResponse(BaseModel):
    """Single page data returned to client."""
    model_config = ConfigDict(from_attributes=True)

    id: int
    document_id: str
    page_number: int
    extracted_text: Optional[str]
    text_length: int
    vlm_used: bool = Field(validation_alias="ocr_used")
    image_path: Optional[str]
    page_type: Optional[str] = None
    is_relevant: Optional[bool] = None
    confidence_source: Optional[str] = None
    vlm_page_type: Optional[str] = None
    vlm_confidence: Optional[str] = None
    vlm_agrees: Optional[bool] = None
    sheet_code: Optional[str] = None
    created_at: datetime


# ── Fixture extraction schemas ────────────────────────────────────────────────

class FixtureResponse(BaseModel):
    """A single luminaire fixture record extracted from a schedule table."""
    code: str = ""
    description: str = ""
    mounting: str = ""
    fixture_style: str = ""
    voltage: str = ""
    lumens: str = ""
    cct: str = ""
    dimming: str = ""
    max_va: str = ""


class FixtureExtractionResponse(BaseModel):
    """Response for fixture extraction endpoint."""
    document_id: str
    total_fixtures: int
    schedule_pages_scanned: int
    plan_pages_scanned: int
    vlm_used: bool
    schedule_sheet_codes: List[str] = []  # sheet codes of schedule pages (e.g. ["E400", "E401"])
    fixtures: List[FixtureResponse]


class DocumentPagesListResponse(BaseModel):
    """All pages for a document."""
    document_id: str
    total_pages: int
    relevant_pages: int = 0
    irrelevant_pages: int = 0
    pages: List[DocumentPageResponse]


class ClassificationSummaryResponse(BaseModel):
    """Summary of page classification for a document."""
    document_id: str
    status: str
    classification_done: bool
    classification_message: Optional[str] = None
    total_pages: int
    relevant_pages: int
    irrelevant_pages: int
    relevant_page_numbers: List[int]
    irrelevant_page_numbers: List[int]
    type_breakdown: dict  # e.g. {"LIGHTING_PLAN": 5, "SCHEDULE": 2, ...}
    relevant_types: List[str]
    irrelevant_types: List[str]
    page_sheet_codes: dict = {}  # {page_number: sheet_code} for pages with sheet codes


# ── Takeoff schemas ───────────────────────────────────────────────────────────

class TakeoffResponse(BaseModel):
    """Response for takeoff generation results."""
    document_id: str
    batch_id: Optional[str] = None       # set when results came from the batch pipeline
    fixture_counts: dict = {}
    total_fixtures_found: int = 0
    overlay_images: List[str] = []
    schedule_csv_available: bool = False
    split_pdf_available: bool = False
    pipeline_status: str = "not_run"  # not_run, running, completed, partial, failed
    source: str = "document"            # "document" or "batch"


class BatchTakeoffResponse(BaseModel):
    """Response for batch-level takeoff results (cross-document)."""
    batch_id: str
    document_ids: List[str] = []
    fixture_counts: dict = {}
    total_fixtures_found: int = 0
    overlay_images: List[str] = []
    schedule_csv_available: bool = False
    split_pdf_available: bool = False
    pipeline_status: str = "not_run"


class BatchDocumentsResponse(BaseModel):
    """List of documents belonging to a batch."""
    batch_id: str
    total: int
    all_completed: bool
    documents: List[DocumentResponse]