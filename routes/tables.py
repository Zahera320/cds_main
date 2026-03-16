"""
Table Extraction Routes
========================
Endpoint for extracting structured table data from relevant PDF pages.

Engine: Docling (pre-extracted tables stored as JSON during processing).

Service:  Table Parser / Table Extraction
Prefix:   /documents
Tag:      5. Table Extraction

Endpoints:
    GET  /documents/{document_id}/tables                  Extract tables from all relevant pages (JSON)
    GET  /documents/{document_id}/tables/fixture-schedules Only fixture schedule tables
    GET  /documents/{document_id}/tables/csv               Extract tables as a downloadable CSV file
    GET  /documents/{document_id}/tables/fixture-schedules/csv  Fixture schedules as CSV
"""

import csv
import io
import json
import logging
import os
import time
import traceback
from collections import defaultdict

from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import StreamingResponse
from sqlalchemy.orm import Session

from utils import get_current_user, get_current_user_flexible, get_db
import models
from services.document_service import DocumentService
from services.storage_service import StorageService
from processing.table_extractor import classify_table, extract_fixtures_from_rows, strip_rows_above_header

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/documents", tags=["6. Tables"])


# ── Per-document result cache ─────────────────────────────────────────────────
_table_cache: dict = {}
_CACHE_MAX = 8


def _load_docling_tables(user_id: int, document_id: str, relevant_pages_map: dict):
    """Load tables from Docling's pre-extracted tables_all.json.

    Returns (result_pages, total_tables, fixture_tables) or None if
    the Docling output does not exist (pre-Docling document).
    """
    pipeline_dir = StorageService.pipeline_dir(user_id, document_id)
    json_path = os.path.join(pipeline_dir, "tables_all.json")

    if not os.path.isfile(json_path):
        return None

    try:
        with open(json_path, "r", encoding="utf-8") as f:
            all_tables = json.load(f)
    except (json.JSONDecodeError, OSError) as exc:
        logger.warning("Failed to load Docling tables JSON: %s", exc)
        return None

    relevant_page_numbers = set(relevant_pages_map.keys())
    pages_tables: dict = defaultdict(list)

    for tbl_entry in all_tables:
        page_no = tbl_entry.get("page_number", 0)
        rows = tbl_entry.get("rows", [])
        if not rows or len(rows) < 2:
            continue

        # Filter to relevant pages only (if we have page info)
        if relevant_page_numbers and page_no not in relevant_page_numbers:
            continue

        cls = classify_table(rows)
        pages_tables[page_no].append({
            "data": rows,
            "classification": cls["classification"],
            "header_label": cls["header_label"],
            "is_fixture_schedule": cls["is_fixture_schedule"],
        })

    result_pages = []
    total_tables = 0
    fixture_tables = 0
    for pn in sorted(pages_tables.keys()):
        page_obj = relevant_pages_map.get(pn)
        table_entries = pages_tables[pn]
        total_tables += len(table_entries)
        fixture_tables += sum(1 for t in table_entries if t["is_fixture_schedule"])
        result_pages.append({
            "page_number": pn,
            "page_type": page_obj.page_type if page_obj else "",
            "sheet_code": (getattr(page_obj, "sheet_code", None) or "") if page_obj else "",
            "table_count": len(table_entries),
            "tables": [
                {
                    "classification": t["classification"],
                    "header_label": t["header_label"],
                    "is_fixture_schedule": t["is_fixture_schedule"],
                    "rows": t["data"],
                }
                for t in table_entries
            ],
        })

    logger.info("Docling tables loaded for %s — %d tables on %d pages",
                document_id, total_tables, len(result_pages))

    return (result_pages, total_tables, fixture_tables)


def _extract_tables_docling(pdf_path, user_id, document_id, relevant_pages_map):
    """Extract tables from a PDF using Docling on-demand (when pre-extracted JSON is unavailable)."""
    from processing.docling_extractor import DoclingExtractor

    page_numbers = sorted(relevant_pages_map.keys())
    cache_key = (pdf_path, tuple(page_numbers))

    if cache_key in _table_cache:
        logger.info("Table cache hit for %s", pdf_path)
        return _table_cache[cache_key]

    t0 = time.time()

    # Run Docling extraction into the pipeline directory
    pipeline_dir = StorageService.pipeline_dir(user_id, document_id)
    extractor = DoclingExtractor(pdf_path, pipeline_dir)
    extractor.run()

    # Now load from the freshly created tables_all.json
    result = _load_docling_tables(user_id, document_id, relevant_pages_map)
    if result is None:
        result = ([], 0, 0)

    elapsed = time.time() - t0
    result_pages, total_tables, fixture_tables = result
    logger.info("On-demand extraction took %.1fs for %s — %d tables on %d pages",
                elapsed, pdf_path, total_tables, len(result_pages))

    if len(_table_cache) >= _CACHE_MAX:
        _table_cache.pop(next(iter(_table_cache)))
    _table_cache[cache_key] = result

    return result





@router.get("/{document_id}/tables")
def get_document_tables(
    document_id: str,
    current_user: models.User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """Extract tables from all **relevant** pages using Docling.

    - Uses Docling for table detection.
    - Operates on the original PDF stored on disk — no re-processing.
    - Only pages where ``is_relevant = true`` are scanned.
    - Each table is returned as a list-of-lists: ``[[row1_cell1, row1_cell2, ...], ...]``
    - Requires ``status = completed`` or ``failed`` (pages must exist).
    """
    try:
        return _extract_tables(db, document_id, current_user.id)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(
            "Error extracting tables for %s: %s\n%s",
            document_id, e, traceback.format_exc(),
        )
        raise HTTPException(status_code=500, detail="Failed to extract tables")


def _extract_tables(db, document_id, user_id):
    """Shared helper that extracts tables and returns the structured result dict."""
    document = DocumentService.get_user_document(db, document_id, user_id)
    if not document:
        raise HTTPException(status_code=404, detail="Document not found or access denied")
    if document.status not in ("completed", "failed"):
        raise HTTPException(
            status_code=400,
            detail="Document processing not yet completed",
        )

    relevant_pages = DocumentService.get_pages(
        db, document_id, skip=0, limit=500, relevance="relevant"
    )
    if not relevant_pages:
        return {"document_id": document_id, "total_tables": 0, "pages": []}

    try:
        pdf_path = StorageService.locate_pdf(user_id, document_id)
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail="PDF file not found on disk")

    # Build lookup: 1-based page number → page DB object
    relevant_pages_map = {p.page_number: p for p in relevant_pages}

    # Try Docling pre-extracted tables first, fall back to on-demand Docling extraction
    engine = "docling"
    result = _load_docling_tables(user_id, document_id, relevant_pages_map)
    if result is None:
        engine = "docling_ondemand"
        result = _extract_tables_docling(pdf_path, user_id, document_id, relevant_pages_map)

    result_pages, total_tables, fixture_tables = result

    # ── VLM fallback: when Docling found 0 fixture schedules ─────────────
    # Docling sometimes misses the LFS table on scanned pages (treats it as
    # flowing text).  When that happens, try Gemini VLM directly on every
    # SCHEDULE-classified relevant page.
    # Results are cached in pipeline/vlm_fixture_cache.json to avoid
    # re-calling Gemini on every subsequent request.
    if fixture_tables == 0:
        try:
            from processing.vlm_classifier import vlm_extract_fixtures, is_vlm_available

            pipeline_dir = StorageService.pipeline_dir(user_id, document_id)
            vlm_cache_path = os.path.join(pipeline_dir, "vlm_fixture_cache.json")

            cached_vlm: list | None = None
            cached_page: int | None = None

            # ── Load from cache (avoids re-calling VLM on every request) ──
            if os.path.isfile(vlm_cache_path):
                try:
                    with open(vlm_cache_path, "r", encoding="utf-8") as _cf:
                        _cache_data = json.load(_cf)
                    cached_vlm = _cache_data.get("fixtures")
                    cached_page = _cache_data.get("page_number")
                    if cached_vlm is not None:
                        logger.info(
                            "VLM fixture cache hit for %s (%d fixtures from page %s)",
                            document_id, len(cached_vlm), cached_page,
                        )
                except Exception as _exc:
                    logger.warning("Failed to read VLM fixture cache: %s", _exc)
                    cached_vlm = None

            if cached_vlm is None and is_vlm_available():
                # ── Run VLM on SCHEDULE pages ────────────────────────────
                schedule_pages = [
                    p for p in relevant_pages
                    if p.page_type in ("SCHEDULE", "LIGHTING_SCHEDULE")
                ]
                for page_obj in schedule_pages:
                    pn = page_obj.page_number
                    logger.info(
                        "VLM fallback: attempting fixture extraction on page %d (SCHEDULE) for %s",
                        pn, document_id,
                    )
                    vlm_fixtures = vlm_extract_fixtures(pdf_path, pn)
                    if vlm_fixtures:
                        cached_vlm = vlm_fixtures
                        cached_page = pn
                        # Persist so subsequent requests skip the VLM call
                        try:
                            with open(vlm_cache_path, "w", encoding="utf-8") as _cf:
                                json.dump({"page_number": pn, "fixtures": vlm_fixtures}, _cf)
                        except Exception as _exc:
                            logger.warning("Failed to write VLM fixture cache: %s", _exc)
                        logger.info(
                            "VLM fallback: extracted %d fixtures from page %d for %s",
                            len(vlm_fixtures), pn, document_id,
                        )
                        break  # found — stop scanning

            # ── Inject cached/fresh VLM fixtures as a synthetic table ────
            if cached_vlm:
                fieldnames = ["code", "description", "fixture_style",
                              "voltage", "mounting", "lumens",
                              "cct", "dimming", "max_va"]
                synthetic_rows = [fieldnames] + [
                    [f.get(k, "") for k in fieldnames] for f in cached_vlm
                ]
                page_obj_for_vlm = next(
                    (p for p in relevant_pages
                     if p.page_number == cached_page),
                    None,
                ) if cached_page else None
                existing_pg = next(
                    (p for p in result_pages if p["page_number"] == cached_page),
                    None,
                ) if cached_page else None
                if existing_pg is None:
                    existing_pg = {
                        "page_number": cached_page,
                        "page_type": (page_obj_for_vlm.page_type or "") if page_obj_for_vlm else "",
                        "sheet_code": (getattr(page_obj_for_vlm, "sheet_code", None) or "") if page_obj_for_vlm else "",
                        "table_count": 0,
                        "tables": [],
                    }
                    result_pages.append(existing_pg)
                existing_pg["tables"].append({
                    "data": synthetic_rows,
                    "classification": "LIGHT_FIXTURE_SCHEDULE",
                    "header_label": "Light Fixture Schedule",
                    "is_fixture_schedule": True,
                    "rows": synthetic_rows,
                    "fixtures": cached_vlm,
                })
                existing_pg["table_count"] = len(existing_pg["tables"])
                total_tables += 1
                fixture_tables += 1
                engine = engine + "+vlm"
        except Exception as exc:
            logger.warning("VLM fixture fallback failed for %s: %s", document_id, exc)

    # ── Run inventory automation on fixture schedule tables only ──────────
    for pg in result_pages:
        for tbl in pg["tables"]:
            if tbl.get("is_fixture_schedule"):
                if "fixtures" not in tbl:
                    # Strip any metadata rows above the actual schedule header
                    tbl["rows"] = strip_rows_above_header(tbl["rows"])
                    # skip_classification=True: table already validated as fixture schedule
                    tbl["fixtures"] = extract_fixtures_from_rows(tbl["rows"], skip_classification=True)
                # rows is the raw data — ensure it is always present
                if "rows" not in tbl:
                    tbl["rows"] = tbl.get("data", [])
            else:
                tbl.setdefault("fixtures", [])

    # ── Group tables by header_label ─────────────────────────────────────
    grouped: dict = defaultdict(list)
    for pg in result_pages:
        for ti, tbl in enumerate(pg["tables"]):
            label = tbl.get("header_label") or tbl.get("classification") or "Other"
            rows = tbl.get("rows") or tbl.get("data") or []
            grouped[label].append({
                "page_number": pg["page_number"],
                "page_type": pg.get("page_type", ""),
                "sheet_code": pg.get("sheet_code", ""),
                "table_index": ti,
                "classification": tbl.get("classification", ""),
                "is_fixture_schedule": tbl.get("is_fixture_schedule", False),
                "rows": rows,
                "fixtures": tbl.get("fixtures", []),
            })

    logger.info(
        "Table extraction for %s: %d tables (%d fixture schedules) on %d pages (engine=%s)",
        document_id, total_tables, fixture_tables, len(result_pages), engine,
    )

    return {
        "document_id": document_id,
        "total_tables": total_tables,
        "fixture_schedule_count": fixture_tables,
        "pages": result_pages,
        "tables_by_header": dict(grouped),
        "engine": engine,
    }


@router.get("/{document_id}/tables/fixture-schedules")
def get_fixture_schedule_tables(
    document_id: str,
    current_user: models.User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """Extract **only Light Fixture Schedule** tables from relevant pages.

    Filters the full table extraction to return only tables classified as
    ``LIGHT_FIXTURE_SCHEDULE``. Each table includes structured inventory
    records (code, description, mounting, voltage, lumens, CCT, dimming, W/VA).

    All other tables (panel schedules, abbreviations, etc.) are still
    stored by the ``/tables`` endpoint but are **not** returned here.
    """
    try:
        data = _extract_tables(db, document_id, current_user.id)

        # Filter tables_by_header to only fixture schedule entries
        fixture_tables = []
        for label, tbls in data.get("tables_by_header", {}).items():
            for tbl in tbls:
                if tbl.get("is_fixture_schedule"):
                    fixture_tables.append({
                        "header_label": label,
                        "page_number": tbl["page_number"],
                        "page_type": tbl.get("page_type", ""),
                        "sheet_code": tbl.get("sheet_code", ""),
                        "rows": tbl["rows"],
                        "fixtures": tbl.get("fixtures", []),
                    })

        return {
            "document_id": document_id,
            "total_tables": len(fixture_tables),
            "tables": fixture_tables,
            "engine": data.get("engine", ""),
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(
            "Error extracting fixture schedule tables for %s: %s\n%s",
            document_id, e, traceback.format_exc(),
        )
        raise HTTPException(status_code=500, detail="Failed to extract fixture schedule tables")


@router.get("/{document_id}/tables/csv")
def get_document_tables_csv(
    document_id: str,
    token: str = None,
    current_user: models.User = Depends(get_current_user_flexible),
    db: Session = Depends(get_db),
):
    """Extract tables from all **relevant** pages and return them as a downloadable CSV file.

    The CSV is structured as follows:
    - Each table is preceded by a header row: ``Page <N> — <page_type> — Table <M>``
    - The first data row of each table is treated as column headers.
    - An empty row separates consecutive tables.

    The response has ``Content-Disposition: attachment`` so browsers will
    prompt the user to save / download the file.
    """
    try:
        data = _extract_tables(db, document_id, current_user.id)

        buf = io.StringIO()
        writer = csv.writer(buf)

        if not data["pages"]:
            writer.writerow(["No tables found on relevant pages"])
        else:
            first = True
            for pg in data["pages"]:
                for ti, tbl in enumerate(pg["tables"]):
                    if not first:
                        writer.writerow([])  # blank separator row
                    first = False
                    # Section header with classification
                    label = tbl.get("header_label", "")
                    classification = tbl.get("classification", "")
                    sc = pg.get("sheet_code", "")
                    sc_label = f" [{sc}]" if sc else ""
                    writer.writerow([
                        f"Page {pg['page_number']}{sc_label} — {pg.get('page_type', 'Unknown')} — Table {ti + 1} — {label} [{classification}]"
                    ])
                    for row in tbl.get("rows", tbl if isinstance(tbl, list) else []):
                        writer.writerow(row)

        buf.seek(0)
        filename = f"tables_{document_id}.csv"
        return StreamingResponse(
            buf,
            media_type="text/csv",
            headers={"Content-Disposition": f'attachment; filename="{filename}"'},
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(
            "Error exporting tables CSV for %s: %s\n%s",
            document_id, e, traceback.format_exc(),
        )
        raise HTTPException(status_code=500, detail="Failed to export tables as CSV")


@router.get("/{document_id}/tables/fixture-schedules/csv")
def get_fixture_schedule_csv(
    document_id: str,
    token: str = None,
    current_user: models.User = Depends(get_current_user_flexible),
    db: Session = Depends(get_db),
):
    """Download **only Light Fixture Schedule** tables as a CSV file.

    Filters the full table extraction to return only tables classified as
    ``LIGHT_FIXTURE_SCHEDULE`` and streams them as a downloadable CSV.
    """
    try:
        data = _extract_tables(db, document_id, current_user.id)

        buf = io.StringIO()
        writer = csv.writer(buf)

        fixture_tables = []
        for label, tbls in data.get("tables_by_header", {}).items():
            for tbl in tbls:
                if tbl.get("is_fixture_schedule"):
                    fixture_tables.append({
                        "header_label": label,
                        "page_number": tbl["page_number"],
                        "page_type": tbl.get("page_type", ""),
                        "sheet_code": tbl.get("sheet_code", ""),
                        "rows": tbl["rows"],
                    })

        if not fixture_tables:
            writer.writerow(["No Light Fixture Schedule tables found"])
        else:
            first = True
            for tbl in fixture_tables:
                if not first:
                    writer.writerow([])  # blank separator row
                first = False
                sc = tbl.get('sheet_code', '')
                sc_label = f" [{sc}]" if sc else ""
                writer.writerow([
                    f"Page {tbl['page_number']}{sc_label} \u2014 {tbl.get('page_type', 'Unknown')} \u2014 {tbl['header_label']}"
                ])
                for row in tbl.get("rows", []):
                    writer.writerow(row)

        buf.seek(0)
        filename = f"light_fixture_schedule_{document_id}.csv"
        return StreamingResponse(
            buf,
            media_type="text/csv",
            headers={"Content-Disposition": f'attachment; filename="{filename}"'},
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(
            "Error exporting fixture schedule CSV for %s: %s\n%s",
            document_id, e, traceback.format_exc(),
        )
        raise HTTPException(status_code=500, detail="Failed to export fixture schedule CSV")
