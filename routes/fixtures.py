"""
Fixture Extraction Routes
==========================
Endpoints for extracting structured luminaire fixture records from
schedule pages in processed PDF documents.

Uses the same Docling-based table extraction as the /tables endpoints
and collects parsed fixture records from LIGHT_FIXTURE_SCHEDULE tables.

Service:  Fixture Extractor
Prefix:   /documents
Tag:      6. Fixture Extraction

Endpoints:
    GET  /documents/{document_id}/fixtures       Extract fixtures (JSON)
"""

import logging
import traceback

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from utils import get_current_user, get_db
import models
from schemas import FixtureResponse, FixtureExtractionResponse
from services.document_service import DocumentService
from routes.tables import _extract_tables

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/documents", tags=["6b. Fixtures"])


@router.get("/{document_id}/fixtures")
def get_document_fixtures(
    document_id: str,
    current_user: models.User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """Extract structured luminaire fixture records from schedule pages.

    Uses Docling-based table extraction to find LIGHT_FIXTURE_SCHEDULE
    tables and return structured fixture records with fields:
    code, description, mounting, fixture_style, voltage, lumens, cct, dimming, max_va
    """
    try:
        return _extract_fixtures(db, document_id, current_user.id)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(
            "Error extracting fixtures for %s: %s\n%s",
            document_id, e, traceback.format_exc(),
        )
        raise HTTPException(status_code=500, detail="Failed to extract fixtures")


def _extract_fixtures(
    db: Session,
    document_id: str,
    user_id: int,
) -> dict:
    """Core extraction logic — powered by Docling via the shared _extract_tables helper."""
    # Use the same Docling-based extraction used by /tables endpoints
    data = _extract_tables(db, document_id, user_id)

    # Count schedule and plan pages scanned
    all_pages = DocumentService.get_all_pages(db, document_id)
    schedule_pages_scanned = sum(
        1 for p in all_pages if p.page_type == "SCHEDULE" and p.is_relevant
    )
    plan_pages_scanned = sum(
        1 for p in all_pages if p.page_type == "LIGHTING_PLAN"
    )
    schedule_sheet_codes = sorted(
        {p.sheet_code for p in all_pages if p.page_type == "SCHEDULE" and p.is_relevant and getattr(p, "sheet_code", None)}
    )

    # Collect all fixture records from fixture schedule tables
    fixture_responses = []
    seen_codes: set = set()

    for label, tbls in data.get("tables_by_header", {}).items():
        for tbl in tbls:
            if not tbl.get("is_fixture_schedule"):
                continue
            for f in tbl.get("fixtures", []):
                # Deduplicate by code
                code_key = (f.get("code") or "").strip().lower()
                if code_key and code_key in seen_codes:
                    continue
                if code_key:
                    seen_codes.add(code_key)
                fixture_responses.append(
                    FixtureResponse(
                        code=f.get("code", ""),
                        description=f.get("description", ""),
                        mounting=f.get("mounting", ""),
                        fixture_style=f.get("fixture_style", ""),
                        voltage=f.get("voltage", ""),
                        lumens=f.get("lumens", ""),
                        cct=f.get("cct", ""),
                        dimming=f.get("dimming", ""),
                        max_va=f.get("max_va", ""),
                    )
                )

    return FixtureExtractionResponse(
        document_id=document_id,
        total_fixtures=len(fixture_responses),
        schedule_pages_scanned=schedule_pages_scanned,
        plan_pages_scanned=plan_pages_scanned,
        vlm_used=False,
        schedule_sheet_codes=schedule_sheet_codes,
        fixtures=fixture_responses,
    ).model_dump()
