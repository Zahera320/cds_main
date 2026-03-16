#!/usr/bin/env python3
"""
QA Pipeline Test Agent
======================
Automated end-to-end testing of the DocuParse PDF processing pipeline.
Tests: upload, classification, VLM verification, table extraction,
fixture parsing, overlay generation, and API response validation.
"""

import json
import os
import sys
import time
import requests
import csv
from io import StringIO
from datetime import datetime

BASE_URL = "http://localhost:8001"
REPORT = []  # Collects test results


def log(msg, level="INFO"):
    ts = datetime.now().strftime("%H:%M:%S")
    prefix = {"INFO": "[INFO]", "PASS": "[PASS]", "FAIL": "[FAIL]", "WARN": "[WARN]"}
    print(f"  {prefix.get(level, '[????]')} {ts} {msg}")
    REPORT.append({"level": level, "message": msg})


def section(title):
    print(f"\n{'='*70}")
    print(f"  {title}")
    print(f"{'='*70}")
    REPORT.append({"level": "SECTION", "message": title})


def wait_for_processing(token, doc_id, max_wait=300):
    """Poll document status until completed or failed."""
    headers = {"Authorization": f"Bearer {token}"}
    start = time.time()
    while time.time() - start < max_wait:
        resp = requests.get(f"{BASE_URL}/documents/{doc_id}", headers=headers)
        if resp.status_code == 200:
            status = resp.json().get("status", "unknown")
            if status == "completed":
                return True
            elif status == "failed":
                log(f"Document {doc_id} processing FAILED", "FAIL")
                return False
        time.sleep(5)
    log(f"Document {doc_id} processing TIMEOUT after {max_wait}s", "FAIL")
    return False


# ── Authentication ────────────────────────────────────────────────────────────

def test_auth():
    section("1. AUTHENTICATION")
    
    # Register
    resp = requests.post(f"{BASE_URL}/auth/register", json={
        "name": "Pipeline QA Agent",
        "email": f"qa_agent_{int(time.time())}@test.com",
        "password": "SecurePass123"
    })
    if resp.status_code == 200:
        log("Registration successful", "PASS")
        user_email = resp.json()["email"]
    else:
        # Try login with existing test account
        log(f"Registration response: {resp.status_code} - trying existing account", "WARN")
        user_email = "qa@test.com"

    # Login
    resp = requests.post(f"{BASE_URL}/auth/login", json={
        "email": user_email,
        "password": "SecurePass123" if "qa_agent" in user_email else "TestPass123"
    })
    if resp.status_code == 200:
        data = resp.json()
        token = data["access_token"]
        log(f"Login successful — token type: {data['token_type']}", "PASS")
        log(f"User ID: {data['user']['id']}, Name: {data['user']['name']}", "INFO")
        
        # Validate token structure
        parts = token.split(".")
        if len(parts) == 3:
            log("JWT token has valid 3-part structure", "PASS")
        else:
            log(f"JWT token has {len(parts)} parts (expected 3)", "FAIL")
        
        return token, data['user']['id']
    else:
        log(f"Login FAILED: {resp.status_code} {resp.text}", "FAIL")
        return None, None


# ── Health Check ──────────────────────────────────────────────────────────────

def test_health():
    section("2. HEALTH CHECK")
    resp = requests.get(f"{BASE_URL}/health")
    if resp.status_code == 200:
        data = resp.json()
        log(f"Status: {data['status']}", "PASS" if data['status'] == 'healthy' else "FAIL")
        log(f"Database: {data['database']}", "PASS" if data['database'] == 'ok' else "FAIL")
        log(f"VLM: {data['vlm']}", "PASS" if data['vlm'] == 'enabled' else "WARN")
        return data['vlm'] == 'enabled'
    else:
        log(f"Health check failed: {resp.status_code}", "FAIL")
        return False


# ── Single PDF Upload & Processing ────────────────────────────────────────────

def test_single_pdf_upload(token):
    section("3. SINGLE PDF UPLOAD")
    headers = {"Authorization": f"Bearer {token}"}
    
    # Find a test PDF - prefer the multi-page electrical CD PDF
    test_pdfs = [
        "/home/ubuntu/project/24031_15_Elec_CD (1).pdf",
        "/home/ubuntu/harshil/input/089---E7A ELECTRICAL SCHEDULES.pdf",
        "/home/ubuntu/harshil/input/Copy of DENIS DENIS-2025-1198(639005170882779184).pdf",
    ]
    
    pdf_path = None
    for p in test_pdfs:
        if os.path.isfile(p):
            pdf_path = p
            break
    
    if not pdf_path:
        log("No test PDF found — skipping single upload test", "WARN")
        return None
    
    log(f"Uploading: {os.path.basename(pdf_path)} ({os.path.getsize(pdf_path)} bytes)")
    
    with open(pdf_path, "rb") as f:
        resp = requests.post(
            f"{BASE_URL}/documents/upload",
            headers=headers,
            files={"file": (os.path.basename(pdf_path), f, "application/pdf")}
        )
    
    if resp.status_code == 200:
        data = resp.json()
        doc_id = data.get("document_id")
        log(f"Upload successful — document_id: {doc_id}", "PASS")
        log(f"Status: {data.get('status')}, File type: {data.get('file_type')}")
        
        # Validate response fields
        required_fields = ["document_id", "filename", "file_type", "status"]
        for field in required_fields:
            if field in data:
                log(f"  Response has '{field}': {data[field]}", "PASS")
            else:
                log(f"  Response missing '{field}'", "FAIL")
        
        # Wait for processing
        log("Waiting for processing to complete...")
        if wait_for_processing(token, doc_id):
            log(f"Document {doc_id} processed successfully", "PASS")
        else:
            log(f"Document {doc_id} processing issue", "FAIL")
        
        return doc_id
    else:
        log(f"Upload FAILED: {resp.status_code} {resp.text[:200]}", "FAIL")
        return None


# ── Multiple PDF Upload ───────────────────────────────────────────────────────

def test_multi_pdf_upload(token):
    section("4. MULTIPLE PDF UPLOAD")
    headers = {"Authorization": f"Bearer {token}"}
    
    test_pdfs = []
    # Use classified PDFs from harshil directory as they're different files
    classified_dir = "/home/ubuntu/harshil/Copy of Electrical Oak Grove City Hall and Fire_classified/RELEVANT/"
    if os.path.isdir(classified_dir):
        for f in sorted(os.listdir(classified_dir)):
            if f.endswith(".pdf"):
                test_pdfs.append(os.path.join(classified_dir, f))
    
    if len(test_pdfs) < 2:
        # Fallback to harshil/input
        input_dir = "/home/ubuntu/harshil/input/"
        if os.path.isdir(input_dir):
            for f in os.listdir(input_dir):
                if f.endswith(".pdf"):
                    test_pdfs.append(os.path.join(input_dir, f))
    
    if len(test_pdfs) < 2:
        log("Not enough PDFs for multi-upload test — skipping", "WARN")
        return None, []
    
    files = []
    for pdf_path in test_pdfs[:3]:  # Test with up to 3 PDFs
        log(f"  Uploading: {os.path.basename(pdf_path)}")
        files.append(("files", (os.path.basename(pdf_path), open(pdf_path, "rb"), "application/pdf")))
    
    resp = requests.post(
        f"{BASE_URL}/documents/upload-multiple",
        headers=headers,
        files=files
    )
    
    # Close file handles
    for _, (_, fh, _) in files:
        fh.close()
    
    if resp.status_code == 200:
        data = resp.json()
        batch_id = data.get("batch_id")
        docs = data.get("results", data.get("documents", []))
        log(f"Multi-upload successful — batch_id: {batch_id}", "PASS")
        log(f"Documents uploaded: {len(docs)}", "PASS")
        
        doc_ids = [d["document_id"] for d in docs]
        
        # Wait for all to complete
        for doc_id in doc_ids:
            log(f"  Waiting for {doc_id}...")
            wait_for_processing(token, doc_id)
        
        return batch_id, doc_ids
    else:
        log(f"Multi-upload FAILED: {resp.status_code} {resp.text[:200]}", "FAIL")
        return None, []


# ── Page Classification Validation ────────────────────────────────────────────

VALID_PAGE_TYPES = {
    "LIGHTING_PLAN", "SCHEDULE", "SYMBOLS_LEGEND", "COVER",
    "DEMOLITION_PLAN", "POWER_PLAN", "SITE_PLAN", "FIRE_ALARM",
    "RISER", "DETAIL", "OTHER"
}

def test_classification(token, doc_id):
    section("5. PAGE CLASSIFICATION VALIDATION")
    if not doc_id:
        log("No document to test — skipping", "WARN")
        return {}
    
    headers = {"Authorization": f"Bearer {token}"}
    
    # Get classification summary
    resp = requests.get(f"{BASE_URL}/documents/{doc_id}/classification-summary", headers=headers)
    if resp.status_code != 200:
        log(f"Classification summary FAILED: {resp.status_code}", "FAIL")
        return {}
    
    data = resp.json()
    log(f"Total pages: {data.get('total_pages', 'N/A')}")
    log(f"Classification done: {data.get('classification_done', 'N/A')}")
    
    summary = data.get("type_breakdown", {})
    log(f"Classification types found: {summary}")
    
    # Validate all types are valid
    invalid_types = []
    for page_type in summary.keys():
        if page_type not in VALID_PAGE_TYPES:
            invalid_types.append(page_type)
    
    if invalid_types:
        log(f"Invalid page types found: {invalid_types}", "FAIL")
    else:
        log("All page types are valid", "PASS")
    
    # Check for SCHEDULE pages
    schedule_count = summary.get("SCHEDULE", 0)
    if schedule_count > 0:
        log(f"SCHEDULE pages detected: {schedule_count}", "PASS")
    else:
        log("No SCHEDULE pages detected", "WARN")
    
    # Check for LIGHTING_PLAN pages
    lp_count = summary.get("LIGHTING_PLAN", 0)
    if lp_count > 0:
        log(f"LIGHTING_PLAN pages detected: {lp_count}", "PASS")
    else:
        log("No LIGHTING_PLAN pages detected", "WARN")
    
    # Get detailed pages
    resp = requests.get(f"{BASE_URL}/documents/{doc_id}/pages", headers=headers)
    if resp.status_code == 200:
        pages_data = resp.json()
        # Handle both list and dict responses
        if isinstance(pages_data, dict):
            pages = pages_data.get("pages", [])
            log(f"Pages response is dict with keys: {list(pages_data.keys())}")
        elif isinstance(pages_data, list):
            pages = pages_data
        else:
            pages = []
        
        log(f"Pages endpoint returned {len(pages)} pages", "PASS" if pages else "WARN")
        
        for page in pages:
            pn = page.get("page_number")
            pt = page.get("page_type")
            vlm_type = page.get("vlm_page_type")
            vlm_conf = page.get("vlm_confidence")
            vlm_agrees = page.get("vlm_agrees")
            text_len = page.get("text_length", 0)
            
            status_parts = [f"Page {pn}: rule={pt}"]
            if vlm_type:
                status_parts.append(f"vlm={vlm_type}({vlm_conf})")
            if vlm_agrees is not None:
                status_parts.append(f"agrees={vlm_agrees}")
            status_parts.append(f"text_len={text_len}")
            
            log("  " + " | ".join(status_parts))
            
            # Check VLM usage
            if vlm_type:
                if vlm_type not in VALID_PAGE_TYPES:
                    log(f"  Page {pn}: VLM returned invalid type: {vlm_type}", "FAIL")
                else:
                    log(f"  Page {pn}: VLM classification valid", "PASS")
    
    return summary


# ── VLM Classifier Agent Testing ──────────────────────────────────────────────

def test_vlm_classifier(token, doc_id, vlm_enabled):
    section("6. VLM CLASSIFIER AGENT VERIFICATION")
    if not doc_id:
        log("No document to test — skipping", "WARN")
        return
    
    if not vlm_enabled:
        log("VLM is disabled — cannot verify VLM agent behavior", "FAIL")
        return
    
    headers = {"Authorization": f"Bearer {token}"}
    
    # Get pages to check VLM results
    resp = requests.get(f"{BASE_URL}/documents/{doc_id}/pages", headers=headers)
    if resp.status_code != 200:
        log(f"Cannot get pages: {resp.status_code}", "FAIL")
        return
    
    pages_data = resp.json()
    if isinstance(pages_data, dict):
        pages = pages_data.get("pages", [])
    elif isinstance(pages_data, list):
        pages = pages_data
    else:
        log("Pages response invalid", "FAIL")
        return
    
    vlm_verified_count = 0
    vlm_disagreed_count = 0
    vlm_override_count = 0
    scanned_pages = []
    
    for page in pages:
        pn = page.get("page_number")
        rule_type = page.get("page_type")
        vlm_type = page.get("vlm_page_type")
        vlm_conf = page.get("vlm_confidence")
        vlm_agrees = page.get("vlm_agrees")
        text_len = page.get("text_length", 0)
        
        if vlm_type:
            vlm_verified_count += 1
            
            if vlm_agrees is False:
                vlm_disagreed_count += 1
                log(f"  Page {pn}: VLM DISAGREED — rule={rule_type}, vlm={vlm_type} (conf={vlm_conf})", "WARN")
                
                # If the final type matches VLM, it was an override
                if rule_type == vlm_type:
                    vlm_override_count += 1
        
        # Detect scanned pages (very low text)
        if text_len is not None and text_len < 50:
            scanned_pages.append(pn)
    
    log(f"VLM verified {vlm_verified_count}/{len(pages)} pages", 
        "PASS" if vlm_verified_count > 0 else "FAIL")
    log(f"VLM disagreements: {vlm_disagreed_count}")
    log(f"VLM overrides: {vlm_override_count}")
    
    if scanned_pages:
        log(f"Scanned pages detected (text < 50 chars): {scanned_pages}", "INFO")
        log("Verifying VLM handles scanned pages...")
        for pn in scanned_pages:
            page = next((p for p in pages if p.get("page_number") == pn), None)
            if page and page.get("vlm_page_type"):
                log(f"  Page {pn}: VLM classified scanned page as {page['vlm_page_type']}", "PASS")
            else:
                log(f"  Page {pn}: VLM did NOT classify scanned page", "WARN")
    else:
        log("No scanned pages detected in this document", "INFO")
    
    # Check VLM agent validation rules
    log("VLM Agent Validation Checks:")
    log(f"  1. VLM processes each page: {'PASS' if vlm_verified_count == len(pages) else 'PARTIAL'}", 
        "PASS" if vlm_verified_count == len(pages) else "WARN")
    log(f"  2. All VLM types valid: checking...")
    
    all_valid = True
    for page in pages:
        vt = page.get("vlm_page_type")
        if vt and vt not in VALID_PAGE_TYPES:
            log(f"     Invalid VLM type on page {page['page_number']}: {vt}", "FAIL")
            all_valid = False
    if all_valid:
        log("  2. All VLM page types are valid enum values", "PASS")
    
    log(f"  3. VLM provides confidence levels: checking...")
    valid_confs = {"high", "medium", "low"}
    conf_ok = True
    for page in pages:
        vc = page.get("vlm_confidence")
        if vc and vc not in valid_confs:
            log(f"     Invalid confidence on page {page['page_number']}: {vc}", "FAIL")
            conf_ok = False
    if conf_ok:
        log("  3. All VLM confidence values are valid", "PASS")


# ── Table Extraction Testing ─────────────────────────────────────────────────

def test_table_extraction(token, doc_id):
    section("7. TABLE EXTRACTION VALIDATION")
    if not doc_id:
        log("No document to test — skipping", "WARN")
        return
    
    headers = {"Authorization": f"Bearer {token}"}
    
    # Get all tables
    resp = requests.get(f"{BASE_URL}/documents/{doc_id}/tables", headers=headers)
    if resp.status_code == 200:
        tables_data = resp.json()
        if isinstance(tables_data, list):
            log(f"Total tables extracted: {len(tables_data)}", "PASS" if tables_data else "WARN")
            for i, tbl in enumerate(tables_data[:5]):
                log(f"  Table {i+1}: page={tbl.get('page_number')}, rows={tbl.get('row_count', 'N/A')}")
        elif isinstance(tables_data, dict):
            log(f"Tables response: {list(tables_data.keys())}")
    elif resp.status_code == 404:
        log("No tables extracted for this document", "WARN")
    else:
        log(f"Tables endpoint error: {resp.status_code}", "FAIL")
    
    # Get fixture schedules specifically
    resp = requests.get(f"{BASE_URL}/documents/{doc_id}/tables/fixture-schedules", headers=headers)
    if resp.status_code == 200:
        data = resp.json()
        log(f"Fixture schedule response received", "PASS")
        
        if isinstance(data, dict):
            fixtures = data.get("fixtures", data.get("tables", []))
            log(f"Fixture schedule entries: {len(fixtures) if isinstance(fixtures, list) else 'N/A'}")
            
            # Check for unwanted table types
            unwanted = ["motor schedule", "panel schedule", "equipment schedule"]
            for key in data:
                for bad in unwanted:
                    if bad.lower() in str(data[key]).lower():
                        log(f"WARNING: Found '{bad}' in fixture schedule response", "WARN")
        elif isinstance(data, list):
            log(f"Fixture schedule entries: {len(data)}")
    elif resp.status_code == 404:
        log("No fixture schedules found", "WARN")
    else:
        log(f"Fixture schedules endpoint error: {resp.status_code} {resp.text[:200]}", "FAIL")
    
    # Get fixture schedule CSV
    resp = requests.get(f"{BASE_URL}/documents/{doc_id}/tables/fixture-schedules/csv", headers=headers)
    if resp.status_code == 200:
        content_type = resp.headers.get("content-type", "")
        if "csv" in content_type or "text/plain" in content_type or "octet-stream" in content_type:
            log("Fixture schedule CSV export works", "PASS")
            # Parse CSV
            try:
                reader = csv.reader(StringIO(resp.text))
                rows = list(reader)
                if rows:
                    log(f"  CSV headers: {rows[0]}")
                    log(f"  CSV data rows: {len(rows) - 1}")
            except Exception as e:
                log(f"  CSV parse error: {e}", "WARN")
        else:
            log(f"CSV export returned unexpected content-type: {content_type}", "WARN")
    elif resp.status_code == 404:
        log("No fixture schedule CSV available", "WARN")
    else:
        log(f"CSV export error: {resp.status_code}", "FAIL")


# ── Fixture Data Validation ───────────────────────────────────────────────────

FIXTURE_FIELDS = ["code", "description", "fixture_style", "voltage", "mounting",
                  "lumens", "cct", "dimming", "max_va"]

def test_fixture_data(token, doc_id):
    section("8. FIXTURE DATA VALIDATION")
    if not doc_id:
        log("No document to test — skipping", "WARN")
        return
    
    headers = {"Authorization": f"Bearer {token}"}
    
    resp = requests.get(f"{BASE_URL}/documents/{doc_id}/fixtures", headers=headers)
    if resp.status_code == 200:
        data = resp.json()
        fixtures = data if isinstance(data, list) else data.get("fixtures", [])
        
        log(f"Fixtures extracted: {len(fixtures)}", "PASS" if fixtures else "WARN")
        
        for i, fix in enumerate(fixtures[:10]):
            log(f"  Fixture {i+1}:")
            
            # Check required fields
            for field in FIXTURE_FIELDS:
                val = fix.get(field)
                if val is not None and val != "":
                    log(f"    {field}: {val}")
                else:
                    log(f"    {field}: MISSING/EMPTY", "WARN")
            
            # Validate fixture code format (should be short like A1, B2, etc.)
            code = fix.get("code", "")
            if code and len(code) <= 5:
                log(f"    Code '{code}' looks valid (short type code)", "PASS")
            elif code:
                log(f"    Code '{code}' might be description instead of type code", "WARN")
    elif resp.status_code == 404:
        log("No fixtures extracted", "WARN")
    else:
        log(f"Fixtures endpoint error: {resp.status_code} {resp.text[:200]}", "FAIL")


# ── Takeoff & Fixture Count Validation ────────────────────────────────────────

def test_takeoff(token, doc_id):
    section("9. TAKEOFF & FIXTURE COUNT VALIDATION")
    if not doc_id:
        log("No document to test — skipping", "WARN")
        return
    
    headers = {"Authorization": f"Bearer {token}"}
    
    # Poll takeoff results — autocount pipeline runs AFTER status becomes "completed",
    # so we may need to wait for it to finish.
    data = None
    for attempt in range(12):
        resp = requests.get(f"{BASE_URL}/documents/{doc_id}/takeoff", headers=headers)
        if resp.status_code == 200:
            data = resp.json()
            status = data.get("pipeline_status", data.get("status", ""))
            if status == "completed":
                break
            elif status in ("running", "partial"):
                log(f"Takeoff pipeline status: {status} — waiting... (attempt {attempt+1}/12)")
                time.sleep(10)
                continue
            else:
                break
        else:
            break
    
    if data:
        log(f"Takeoff response received", "PASS")
        
        if isinstance(data, dict):
            # Check fixture counts
            counts = data.get("fixture_counts", data.get("counts", {}))
            if counts:
                log(f"Fixture counts found:", "PASS")
                if isinstance(counts, dict):
                    for code, count in counts.items():
                        log(f"    {code}: {count}")
                elif isinstance(counts, list):
                    for item in counts:
                        log(f"    {item}")
            else:
                log("No fixture counts in takeoff response", "WARN")
            
            # Check for overlay paths
            overlays = data.get("overlays", data.get("overlay_paths", []))
            if overlays:
                log(f"Overlay paths: {len(overlays)}", "PASS")
            
            # Check processing status
            status = data.get("status", data.get("pipeline_status"))
            if status:
                log(f"Pipeline status: {status}")
            
            # Log all available keys
            log(f"Takeoff response keys: {list(data.keys())}")
    else:
        if resp.status_code == 404:
            log("No takeoff results available", "WARN")
        else:
            log(f"Takeoff endpoint error: {resp.status_code} {resp.text[:200]}", "FAIL")
    
    # Check schedule CSV
    resp = requests.get(f"{BASE_URL}/documents/{doc_id}/takeoff/schedule-csv", headers=headers)
    if resp.status_code == 200:
        log("Schedule CSV available via takeoff endpoint", "PASS")
        try:
            reader = csv.reader(StringIO(resp.text))
            rows = list(reader)
            log(f"  Schedule CSV rows: {len(rows)}")
        except:
            pass
    elif resp.status_code == 404:
        log("No schedule CSV from takeoff", "WARN")
    else:
        log(f"Schedule CSV error: {resp.status_code}", "FAIL")
    
    # Check matrix CSV
    resp = requests.get(f"{BASE_URL}/documents/{doc_id}/takeoff/matrix-csv", headers=headers)
    if resp.status_code == 200:
        log("Matrix CSV available", "PASS")
    elif resp.status_code == 404:
        log("No matrix CSV available", "WARN")
    else:
        log(f"Matrix CSV error: {resp.status_code}", "FAIL")


# ── Overlay Validation ────────────────────────────────────────────────────────

def test_overlays(token, doc_id):
    section("10. OVERLAY VALIDATION")
    if not doc_id:
        log("No document to test — skipping", "WARN")
        return
    
    headers = {"Authorization": f"Bearer {token}"}
    
    # First get the takeoff results to find available sheet IDs
    resp_t = requests.get(f"{BASE_URL}/documents/{doc_id}/takeoff", headers=headers)
    sheet_id = None
    if resp_t.status_code == 200:
        t_data = resp_t.json()
        sheets = t_data.get("available_sheets", [])
        if sheets:
            sheet_id = sheets[0]
    
    if not sheet_id:
        sheet_id = "1"  # fallback
    
    # Get overlay for a specific sheet
    resp = requests.get(f"{BASE_URL}/documents/{doc_id}/takeoff/overlay", headers=headers,
                       params={"sheet_id": sheet_id})
    if resp.status_code == 200:
        content_type = resp.headers.get("content-type", "")
        content_length = len(resp.content)
        if "image" in content_type or content_length > 1000:
            log(f"Overlay image received: {content_length} bytes, type={content_type}", "PASS")
        else:
            log(f"Overlay response unexpected: {content_type}, {content_length} bytes", "WARN")
    elif resp.status_code == 404:
        log("No overlay images generated (may be expected if no lighting plans)", "WARN")
    else:
        log(f"Overlay endpoint error: {resp.status_code}", "FAIL")
    
    # Check Docling outputs
    resp = requests.get(f"{BASE_URL}/documents/{doc_id}/takeoff/docling-outputs", headers=headers)
    if resp.status_code == 200:
        data = resp.json()
        log(f"Docling outputs available: {list(data.keys()) if isinstance(data, dict) else 'list'}", "PASS")
    elif resp.status_code == 404:
        log("No Docling outputs available", "WARN")
    else:
        log(f"Docling outputs error: {resp.status_code}", "FAIL")


# ── Folder Output Validation ─────────────────────────────────────────────────

def test_folder_structure(token, doc_id, user_id):
    section("11. FOLDER OUTPUT VALIDATION")
    if not doc_id or not user_id:
        log("No document to test — skipping", "WARN")
        return
    
    storage_base = "/home/ubuntu/cds-main/project/app.py/storage"
    doc_dir = os.path.join(storage_base, str(user_id), doc_id)
    
    if not os.path.isdir(doc_dir):
        log(f"Document storage directory not found: {doc_dir}", "FAIL")
        return
    
    log(f"Document directory: {doc_dir}", "PASS")
    
    # Check expected subdirectories
    expected_dirs = ["original", "pages", "pipeline"]
    for d in expected_dirs:
        path = os.path.join(doc_dir, d)
        if os.path.isdir(path):
            files = os.listdir(path)
            log(f"  {d}/: {len(files)} files", "PASS")
        else:
            log(f"  {d}/: NOT FOUND", "WARN")
    
    # Check pipeline subdirectories
    pipeline_dir = os.path.join(doc_dir, "pipeline")
    if os.path.isdir(pipeline_dir):
        pipeline_contents = os.listdir(pipeline_dir)
        log(f"  pipeline/ contents: {pipeline_contents[:20]}")
        
        # Check for key pipeline outputs
        key_files = {
            "lighting_schedule.csv": "Light Fixture Schedule CSV",
            "lighting_panel_plans.pdf": "Split Lighting Plans PDF",
            "fixture_counts.json": "Fixture Counts JSON",
            "combined_text_table.txt": "Combined Text+Table File",
        }
        for fname, desc in key_files.items():
            fpath = os.path.join(pipeline_dir, fname)
            if os.path.isfile(fpath):
                size = os.path.getsize(fpath)
                log(f"  {fname}: {size} bytes — {desc}", "PASS")
            else:
                log(f"  {fname}: NOT FOUND — {desc}", "WARN")
        
        # Check for overlay images
        overlays = [f for f in pipeline_contents if f.startswith("output_overlay_") and f.endswith(".png")]
        if overlays:
            log(f"  Overlay images: {len(overlays)} files", "PASS")
            for ov in overlays[:5]:
                size = os.path.getsize(os.path.join(pipeline_dir, ov))
                log(f"    {ov}: {size} bytes")
        else:
            log("  No overlay images generated", "WARN")
        
        # Check relevant_tables subfolder
        rel_tables = os.path.join(pipeline_dir, "relevant_tables")
        if os.path.isdir(rel_tables):
            files = os.listdir(rel_tables)
            log(f"  relevant_tables/: {files}", "PASS")
        else:
            log("  relevant_tables/: NOT CREATED", "WARN")
        
        # Check fixture_results subfolder
        fix_results = os.path.join(pipeline_dir, "fixture_results")
        if os.path.isdir(fix_results):
            files = os.listdir(fix_results)
            log(f"  fixture_results/: {files}", "PASS")
        else:
            log("  fixture_results/: NOT CREATED", "WARN")
        
        # Validate fixture_counts.json content
        counts_path = os.path.join(pipeline_dir, "fixture_counts.json")
        if os.path.isfile(counts_path):
            try:
                with open(counts_path, "r") as f:
                    counts = json.load(f)
                log(f"  fixture_counts.json is valid JSON", "PASS")
                log(f"  Counts data: {json.dumps(counts, indent=2)[:500]}")
            except json.JSONDecodeError as e:
                log(f"  fixture_counts.json is INVALID JSON: {e}", "FAIL")


# ── Scanned PDF Testing ──────────────────────────────────────────────────────

def test_scanned_pdf(token):
    section("12. SCANNED PDF VERIFICATION")
    headers = {"Authorization": f"Bearer {token}"}
    
    # Use the second schedule PDF to avoid duplicate detection
    scanned_pdf = "/home/ubuntu/harshil/input/090---E7B ELECTRICAL SCHEDULES.pdf"
    if not os.path.isfile(scanned_pdf):
        scanned_pdf = "/home/ubuntu/harshil/input/089---E7A ELECTRICAL SCHEDULES.pdf"
    
    if not os.path.isfile(scanned_pdf):
        log(f"Scanned test PDF not found", "WARN")
        return None
    
    log(f"Testing with: {os.path.basename(scanned_pdf)}")
    
    # Upload scanned PDF
    with open(scanned_pdf, "rb") as f:
        resp = requests.post(
            f"{BASE_URL}/documents/upload",
            headers=headers,
            files={"file": (os.path.basename(scanned_pdf), f, "application/pdf")}
        )
    
    if resp.status_code != 200:
        log(f"Upload failed: {resp.status_code} {resp.text[:200]}", "FAIL")
        return None
    
    doc_id = resp.json().get("document_id")
    log(f"Uploaded — doc_id: {doc_id}", "PASS")
    
    # Wait for processing
    log("Waiting for scanned PDF processing...")
    if not wait_for_processing(token, doc_id, max_wait=600):
        return doc_id
    
    # Check pages for scanned characteristics
    resp = requests.get(f"{BASE_URL}/documents/{doc_id}/pages", headers=headers)
    if resp.status_code == 200:
        pages_data = resp.json()
        pages = pages_data.get("pages", pages_data) if isinstance(pages_data, dict) else pages_data
        if not isinstance(pages, list):
            pages = []
        for page in pages:
            pn = page.get("page_number")
            text_len = page.get("text_length", 0)
            ocr = page.get("ocr_used", False)
            pt = page.get("page_type")
            vlm_type = page.get("vlm_page_type")
            vlm_conf = page.get("vlm_confidence")
            
            is_scanned = text_len is not None and text_len < 100
            
            log(f"  Page {pn}: text_len={text_len}, ocr={ocr}, type={pt}, vlm={vlm_type}")
            
            if is_scanned:
                log(f"  Page {pn}: Detected as scanned (low text)", "INFO")
                if vlm_type:
                    log(f"  Page {pn}: VLM handled scanned page → {vlm_type} (conf={vlm_conf})", "PASS")
                else:
                    log(f"  Page {pn}: VLM did NOT process scanned page", "WARN")
    
    # Check if schedule extraction still works for scanned pages
    resp = requests.get(f"{BASE_URL}/documents/{doc_id}/tables/fixture-schedules", headers=headers)
    if resp.status_code == 200:
        log("Schedule extraction attempted on scanned document", "PASS")
    else:
        log(f"Schedule extraction on scanned doc: {resp.status_code}", "WARN")
    
    return doc_id


# ── Reclassification Test ────────────────────────────────────────────────────

def test_reclassification(token, doc_id):
    section("13. RECLASSIFICATION TEST")
    if not doc_id:
        log("No document to test — skipping", "WARN")
        return
    
    headers = {"Authorization": f"Bearer {token}"}
    
    # Trigger reclassification
    resp = requests.post(f"{BASE_URL}/documents/{doc_id}/reclassify", headers=headers)
    if resp.status_code == 200:
        log("Reclassification triggered successfully", "PASS")
        # Wait for reprocessing
        time.sleep(10)
        
        # Verify classification updated
        resp2 = requests.get(f"{BASE_URL}/documents/{doc_id}/classification-summary", headers=headers)
        if resp2.status_code == 200:
            data = resp2.json()
            log(f"Post-reclassify summary: {data.get('classification_summary', {})}", "PASS")
    else:
        log(f"Reclassification error: {resp.status_code} {resp.text[:200]}", "FAIL")


# ── Batch Takeoff Validation ─────────────────────────────────────────────────

def test_batch_takeoff(token, batch_id):
    section("14. BATCH TAKEOFF VALIDATION")
    if not batch_id:
        log("No batch to test — skipping", "WARN")
        return
    
    headers = {"Authorization": f"Bearer {token}"}
    
    # Get batch documents
    resp = requests.get(f"{BASE_URL}/batch/{batch_id}/documents", headers=headers)
    if resp.status_code == 200:
        docs = resp.json()
        log(f"Batch has {len(docs) if isinstance(docs, list) else 'N/A'} documents", "PASS")
    else:
        log(f"Batch documents error: {resp.status_code}", "FAIL")
    
    # Get batch takeoff
    resp = requests.get(f"{BASE_URL}/batch/{batch_id}/takeoff", headers=headers)
    if resp.status_code == 200:
        data = resp.json()
        log(f"Batch takeoff available: {list(data.keys()) if isinstance(data, dict) else 'ok'}", "PASS")
    elif resp.status_code == 404:
        log("No batch takeoff results yet", "WARN")
    else:
        log(f"Batch takeoff error: {resp.status_code}", "FAIL")


# ── Processing Logs ──────────────────────────────────────────────────────────

def test_processing_logs(token, doc_id):
    section("15. PROCESSING LOGS VALIDATION")
    if not doc_id:
        log("No document to test — skipping", "WARN")
        return
    
    headers = {"Authorization": f"Bearer {token}"}
    
    resp = requests.get(f"{BASE_URL}/documents/{doc_id}/logs", headers=headers)
    if resp.status_code == 200:
        try:
            data = resp.json()
            if isinstance(data, dict):
                log_text = data.get("log", data.get("logs", ""))
                if log_text:
                    lines = log_text.strip().split("\n") if isinstance(log_text, str) else log_text
                    log(f"Processing log: {len(lines)} lines", "PASS")
                    for line in (lines[-10:] if isinstance(lines, list) else log_text.strip().split("\n")[-10:]):
                        log(f"    {line}")
                else:
                    log("Processing log empty", "WARN")
            elif isinstance(data, str):
                lines = data.strip().split("\n")
                log(f"Processing log: {len(lines)} lines", "PASS")
                for line in lines[-10:]:
                    log(f"    {line}")
            else:
                log(f"Logs response type: {type(data)}", "INFO")
        except Exception:
            # Response might be plain text
            text = resp.text
            if text:
                lines = text.strip().split("\n")
                log(f"Processing log (text): {len(lines)} lines", "PASS")
                for line in lines[-10:]:
                    log(f"    {line}")
            else:
                log("Processing log empty", "WARN")
    elif resp.status_code == 404:
        log("No processing logs available", "WARN")
    else:
        log(f"Logs error: {resp.status_code}", "FAIL")


# ── Error Detection ──────────────────────────────────────────────────────────

def check_for_errors(token, doc_ids):
    section("16. ERROR DETECTION SWEEP")
    headers = {"Authorization": f"Bearer {token}"}
    
    issues = []
    
    for doc_id in doc_ids:
        if not doc_id:
            continue
        
        # Check document status
        resp = requests.get(f"{BASE_URL}/documents/{doc_id}", headers=headers)
        if resp.status_code == 200:
            status = resp.json().get("status")
            if status == "failed":
                issues.append(f"Document {doc_id}: status=FAILED")
        
        # Check for classification issues
        resp = requests.get(f"{BASE_URL}/documents/{doc_id}/pages", headers=headers)
        if resp.status_code == 200:
            pages_data = resp.json()
            pages = pages_data.get("pages", pages_data) if isinstance(pages_data, dict) else pages_data
            if isinstance(pages, list):
                for page in pages:
                    pn = page.get("page_number")
                    pt = page.get("page_type")
                    if not pt:
                        issues.append(f"Doc {doc_id}, Page {pn}: no page_type")
                    if pt and pt not in VALID_PAGE_TYPES:
                        issues.append(f"Doc {doc_id}, Page {pn}: invalid type '{pt}'")
    
    if issues:
        log(f"Found {len(issues)} issues:", "FAIL")
        for issue in issues:
            log(f"  - {issue}", "FAIL")
    else:
        log("No critical issues detected", "PASS")
    
    return issues


# ── Final Report ─────────────────────────────────────────────────────────────

def generate_report():
    section("QA REPORT SUMMARY")
    
    passes = sum(1 for r in REPORT if r["level"] == "PASS")
    fails = sum(1 for r in REPORT if r["level"] == "FAIL")
    warns = sum(1 for r in REPORT if r["level"] == "WARN")
    
    print(f"\n  Total Checks:  {passes + fails + warns}")
    print(f"  PASSED:        {passes}")
    print(f"  FAILED:        {fails}")
    print(f"  WARNINGS:      {warns}")
    
    if fails > 0:
        print(f"\n  FAILURES:")
        for r in REPORT:
            if r["level"] == "FAIL":
                print(f"    - {r['message']}")
    
    if warns > 0:
        print(f"\n  WARNINGS:")
        for r in REPORT:
            if r["level"] == "WARN":
                print(f"    - {r['message']}")
    
    # Write full report to file
    report_path = "/tmp/qa_pipeline_report.json"
    with open(report_path, "w") as f:
        json.dump({
            "timestamp": datetime.now().isoformat(),
            "summary": {"passes": passes, "fails": fails, "warnings": warns},
            "details": REPORT
        }, f, indent=2)
    print(f"\n  Full report saved to: {report_path}")
    
    return fails == 0


# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    print("=" * 70)
    print("  DocuParse QA Pipeline Test Agent")
    print("  Testing full PDF processing pipeline end-to-end")
    print("=" * 70)
    
    # 1. Health check
    vlm_enabled = test_health()
    
    # 2. Auth
    token, user_id = test_auth()
    if not token:
        print("FATAL: Cannot authenticate — aborting tests")
        return False
    
    # 3. Single PDF upload
    doc_id_single = test_single_pdf_upload(token)
    
    # 4. Classification
    summary = test_classification(token, doc_id_single)
    
    # 5. VLM classifier
    test_vlm_classifier(token, doc_id_single, vlm_enabled)
    
    # 6. Table extraction
    test_table_extraction(token, doc_id_single)
    
    # 7. Fixture data
    test_fixture_data(token, doc_id_single)
    
    # 8. Takeoff
    test_takeoff(token, doc_id_single)
    
    # 9. Overlays
    test_overlays(token, doc_id_single)
    
    # 10. Folder structure
    test_folder_structure(token, doc_id_single, user_id)
    
    # 11. Scanned PDF
    doc_id_scanned = test_scanned_pdf(token)
    
    # 12. Multi PDF upload
    batch_id, multi_doc_ids = test_multi_pdf_upload(token)
    
    # 13. Reclassification
    test_reclassification(token, doc_id_single)
    
    # 14. Batch takeoff
    test_batch_takeoff(token, batch_id)
    
    # 15. Processing logs
    test_processing_logs(token, doc_id_single)
    
    # 16. Error sweep
    all_doc_ids = [doc_id_single, doc_id_scanned] + multi_doc_ids
    check_for_errors(token, [d for d in all_doc_ids if d])
    
    # Report
    return generate_report()


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
