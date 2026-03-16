"""
VLM Page Classifier  (Vision Language Model Re-Verification Layer)
===================================================================
Uses Google Gemini (gemini-3.1-pro-preview) to verify, override, and enhance
page classifications by analysing rendered page images.

This module acts as a *re-verification layer* on top of rule-based
classification.  For every page it:
    1. Renders the page to a temporary image (or reuses the existing PNG).
    2. Sends the image + a structured prompt to Gemini.
    3. Parses the JSON response to extract the VLM's classification
       and confidence score.
    4. **Overrides** the rule-based result when VLM has high confidence
       and disagrees — the VLM is the final authority on page type.

Additional capabilities:
    - Table verification: confirms whether a schedule page contains a
      Light Fixture Schedule vs other schedule types.
    - Scanned page detection: identifies pages with no extractable text.
    - Gemini-based table extraction: fallback when Docling extraction
      fails or returns empty/low-quality results for schedule pages.

Public API
    vlm_classify_page(pdf_path, page_idx, existing_type) → dict
    vlm_verify_all_pages(pdf_path, total_pages, rule_results, pages_dir) → dict
    vlm_verify_table(pdf_path, page_number, table_rows) → dict
    vlm_extract_table(pdf_path, page_number) → dict | None
    vlm_extract_fixtures(pdf_path, page_number) → list[dict]
    detect_scanned_pages(page_results) → list[int]
"""

import json
import io
import logging
import os
import re
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeoutError, as_completed
from typing import Any, Dict, Optional

import fitz  # PyMuPDF
from processing.page_classifier import PageType

logger = logging.getLogger(__name__)

# ── Constants ─────────────────────────────────────────────────────────────────
_VLM_TIMEOUT = 60           # seconds — max time for a classification Gemini call
_VLM_EXTRACT_TIMEOUT = 180  # seconds — max time for a table-extraction Gemini call
_VLM_MAX_RETRIES = 2        # retries on JSON parse / API failure
_VLM_INTER_PAGE_DELAY = 0.3  # seconds between page calls to avoid rate limits
_VLM_EXTRACT_MAX_TOKENS = 32768  # output token budget for table extraction
# Cap the longest pixel dimension of images sent to Gemini for extraction.
# Large-format drawings at 200 DPI can be 5000×7500 px; capping keeps latency low.
_VLM_EXTRACT_MAX_DIM = 2048

# ── Lazy-loaded Gemini client ─────────────────────────────────────────────────
_gemini_model = None


_GEMINI_MODEL_NAME = "gemini-2.5-flash"


def _get_model():
    """Return a cached Gemini GenerativeModel, or None if unavailable."""
    global _gemini_model
    if _gemini_model is not None:
        return _gemini_model

    api_key = os.getenv("GOOGLE_API_KEY", "").strip()
    if not api_key:
        logger.warning("GOOGLE_API_KEY not set — VLM verification disabled")
        return None

    try:
        import google.generativeai as genai
        genai.configure(api_key=api_key)
        _gemini_model = genai.GenerativeModel(_GEMINI_MODEL_NAME)
        logger.info("Gemini VLM initialised (google-generativeai — %s)", _GEMINI_MODEL_NAME)
        return _gemini_model
    except Exception as exc:
        logger.error("Failed to initialise Gemini VLM: %s", exc)
        return None


def is_vlm_available() -> bool:
    """Return True if the VLM backend is configured (key is set)."""
    api_key = os.getenv("GOOGLE_API_KEY", "").strip()
    return bool(api_key)


# ── Valid page types (derived from the canonical PageType enum) ───────────────
_VALID_TYPES = {pt.value for pt in PageType}

_CLASSIFICATION_PROMPT = """You are an expert electrical/architectural engineer reviewing construction drawing pages.

Analyse the attached page image and classify it into EXACTLY ONE of these categories:

- LIGHTING_PLAN    : Floor plan showing lighting fixtures layout  
- SCHEDULE         : Tabular schedule (luminaire, panel, equipment)
- SYMBOLS_LEGEND   : Symbols list, abbreviations, or legend page
- COVER            : Title/cover sheet with project info
- DEMOLITION_PLAN  : Demolition plan (indicates items to remove)
- POWER_PLAN       : Power/receptacle plan layout
- SITE_PLAN        : Site plan or photometric plan
- FIRE_ALARM       : Fire alarm devices layout
- RISER            : Electrical riser diagram
- DETAIL           : Detail drawings / enlarged sections
- OTHER            : None of the above

The current rule-based system classified this page as: "{rule_type}"

Respond with ONLY valid JSON (no markdown fences):
{{"page_type": "<TYPE>", "confidence": "<high|medium|low>", "reasoning": "<one sentence>"}}
"""


def _render_page_image(
    pdf_path: str,
    page_idx: int,
    dpi: int = 150,
    max_dim: Optional[int] = None,
) -> Optional[bytes]:
    """Render a single page to PNG bytes.

    Args:
        dpi:     150 for classification, 200 for table extraction.
        max_dim: If set, downsample so neither dimension exceeds this value.
                 Keeps API payloads small for large-format drawings.
    """
    try:
        doc = fitz.open(pdf_path)
        page = doc.load_page(page_idx)
        pix = page.get_pixmap(dpi=dpi)
        img_bytes = pix.tobytes("png")
        pix = None  # release pixmap memory
        doc.close()

        if max_dim and img_bytes:
            from PIL import Image as _PILImage
            pil = _PILImage.open(io.BytesIO(img_bytes))
            w, h = pil.size
            if w > max_dim or h > max_dim:
                scale = max_dim / max(w, h)
                new_size = (max(1, int(w * scale)), max(1, int(h * scale)))
                pil = pil.resize(new_size, _PILImage.LANCZOS)
                buf = io.BytesIO()
                pil.save(buf, "PNG")
                img_bytes = buf.getvalue()
                logger.debug(
                    "Page %d resized %dx%d → %dx%d for VLM",
                    page_idx + 1, w, h, new_size[0], new_size[1],
                )

        return img_bytes
    except Exception as exc:
        logger.error("Failed to render page %d for VLM: %s", page_idx + 1, exc)
        return None


def _load_existing_image(image_path: str) -> Optional[bytes]:
    """Load an already-rendered page image from disk."""
    try:
        if image_path and os.path.isfile(image_path):
            with open(image_path, "rb") as f:
                return f.read()
    except Exception:
        pass
    return None


def _robust_json_parse(text: str) -> Optional[Dict]:
    """
    Parse JSON from a Gemini response, handling common quirks:
      - Markdown code fences (```json ... ```)
      - Leading/trailing prose around the JSON object
      - Trailing commas inside arrays/objects
      - Single-quoted strings (rare, but seen on retries)
    Returns the parsed dict, or None.
    """
    if not text:
        return None

    cleaned = text.strip()

    # 1. Strip markdown code fences (possibly nested or repeated)
    cleaned = re.sub(r'```(?:json)?\s*', '', cleaned)
    cleaned = re.sub(r'\s*```', '', cleaned)
    cleaned = cleaned.strip()

    # 2. Try direct parse first (fastest path)
    try:
        return json.loads(cleaned)
    except json.JSONDecodeError:
        pass

    # 3. Extract the first top-level JSON object from surrounding prose
    m = re.search(r'\{', cleaned)
    if m:
        depth = 0
        start = m.start()
        in_str = False
        esc = False
        for i in range(start, len(cleaned)):
            c = cleaned[i]
            if esc:
                esc = False
                continue
            if c == '\\':
                esc = True
                continue
            if c == '"':
                in_str = not in_str
                continue
            if in_str:
                continue
            if c == '{':
                depth += 1
            elif c == '}':
                depth -= 1
                if depth == 0:
                    candidate = cleaned[start:i + 1]
                    try:
                        return json.loads(candidate)
                    except json.JSONDecodeError:
                        # 4. Fix trailing commas: ,] or ,}
                        fixed = re.sub(r',\s*([}\]])', r'\1', candidate)
                        try:
                            return json.loads(fixed)
                        except json.JSONDecodeError:
                            pass
                    break

    # 5. Partial fixtures-array recovery: brace-track complete objects from a
    #    truncated "fixtures": [ array.  Runs BEFORE scalar key-value recovery
    #    (step 6) because step 6 matches field names nested inside fixture
    #    objects (code, description, voltage, …) and returns a flat dict with
    #    no "fixtures" key, making it look like no schedule was found even when
    #    Gemini was mid-way through emitting one.
    arr_m = re.search(r'"fixtures"\s*:\s*\[', cleaned)
    if arr_m:
        arr_start = arr_m.end()
        fragment = cleaned[arr_start:]
        partial_fixtures = []
        pos = 0
        while pos < len(fragment):
            while pos < len(fragment) and fragment[pos] in ' \t\n\r,':
                pos += 1
            if pos >= len(fragment) or fragment[pos] != '{':
                break
            obj_start = pos
            depth = 0
            in_str = False
            esc = False
            for i in range(pos, len(fragment)):
                c = fragment[i]
                if esc:
                    esc = False
                    continue
                if c == '\\':
                    esc = True
                    continue
                if c == '"':
                    in_str = not in_str
                    continue
                if in_str:
                    continue
                if c == '{':
                    depth += 1
                elif c == '}':
                    depth -= 1
                    if depth == 0:
                        candidate = fragment[obj_start:i + 1]
                        try:
                            obj = json.loads(candidate)
                            partial_fixtures.append(obj)
                        except json.JSONDecodeError:
                            fixed = re.sub(r',\s*([}\]])', r'\1', candidate)
                            try:
                                obj = json.loads(fixed)
                                partial_fixtures.append(obj)
                            except json.JSONDecodeError:
                                pass
                        pos = i + 1
                        break
            else:
                break
        if partial_fixtures:
            logger.info(
                "_robust_json_parse: recovered %d fixtures from truncated array",
                len(partial_fixtures),
            )
            return {"table_type": "light_fixture_schedule", "fixtures": partial_fixtures}

    # 6. Last-resort scalar recovery for classification/verification responses.
    #    (page_type, confidence, has_fixture_schedule, etc.)
    m2 = re.search(r'\{', cleaned)
    if m2:
        fragment = cleaned[m2.start():]
        recovered = {}
        for k, v in re.findall(r'"(\w+)"\s*:\s*"([^"]*)"', fragment):
            recovered[k] = v
        for k, v in re.findall(r'"(\w+)"\s*:\s*(true|false)\b', fragment, re.IGNORECASE):
            recovered[k] = v.lower() == "true"
        for k, v in re.findall(r'"(\w+)"\s*:\s*(\d+)(?:\s*[,}])', fragment):
            recovered[k] = int(v)
        if recovered:
            logger.info("_robust_json_parse: recovered %d fields from truncated JSON", len(recovered))
            return recovered

    logger.warning("_robust_json_parse: could not extract JSON from: %s", text[:300])
    return None


def _parse_vlm_response(text: str) -> Optional[Dict[str, str]]:
    """
    Parse the classification JSON response from Gemini.
    """
    data = _robust_json_parse(text)
    if data is None:
        return None

    page_type = data.get("page_type", "").upper().strip()
    confidence = data.get("confidence", "low").lower().strip()
    reasoning = data.get("reasoning", "")

    if page_type not in _VALID_TYPES:
        logger.warning(
            "VLM returned invalid type '%s' — falling back to OTHER",
            page_type,
        )
        page_type = PageType.OTHER.value

    if confidence not in ("high", "medium", "low"):
        confidence = "low"

    return {
        "vlm_page_type": page_type,
        "vlm_confidence": confidence,
        "vlm_reasoning": reasoning,
    }


def vlm_classify_page(
    pdf_path: str,
    page_idx: int,
    rule_type: str,
    existing_image_path: Optional[str] = None,
) -> Optional[Dict[str, str]]:
    """
    Send a single page image to Gemini for classification verification.

    Args:
        pdf_path:            Path to the PDF file.
        page_idx:            Zero-based page index.
        rule_type:           The rule-based classification result (e.g. "LIGHTING_PLAN").
        existing_image_path: Optional path to an already-rendered PNG.

    Returns:
        {"vlm_page_type": str, "vlm_confidence": str, "vlm_reasoning": str}
        or None if VLM is unavailable or the call fails.
    """
    client = _get_model()
    if client is None:
        return None

    # Load or render the page image
    img_bytes = _load_existing_image(existing_image_path) if existing_image_path else None
    if img_bytes is None:
        img_bytes = _render_page_image(pdf_path, page_idx)
    if img_bytes is None:
        return None

    prompt = _CLASSIFICATION_PROMPT.format(rule_type=rule_type)

    import time as _time
    try:
        import google.generativeai as genai
        from PIL import Image

        # Convert bytes to PIL Image, then release raw bytes
        pil_image = Image.open(io.BytesIO(img_bytes))
        pil_image.load()
        img_bytes = None

        generation_config = genai.GenerationConfig(
            temperature=0.1,
            max_output_tokens=512,
        )

        def _call_gemini():
            return client.generate_content(
                [pil_image, prompt],
                generation_config=generation_config,
                request_options={"timeout": _VLM_TIMEOUT},
            )

        # Retry loop: attempt up to _VLM_MAX_RETRIES + 1 times on JSON parse failure
        for attempt in range(_VLM_MAX_RETRIES + 1):
            try:
                with ThreadPoolExecutor(max_workers=1) as pool:
                    future = pool.submit(_call_gemini)
                    try:
                        response = future.result(timeout=_VLM_TIMEOUT + 10)
                    except FuturesTimeoutError:
                        logger.error("VLM call timed out for page %d (>%ds)", page_idx + 1, _VLM_TIMEOUT)
                        return None

                # Extract text from response, handling safety blocks
                resp_text = None
                try:
                    resp_text = response.text
                except (ValueError, AttributeError):
                    # Response may have been blocked by safety filters
                    if hasattr(response, 'candidates') and response.candidates:
                        for candidate in response.candidates:
                            if hasattr(candidate, 'content') and candidate.content:
                                for part in candidate.content.parts:
                                    if hasattr(part, 'text') and part.text:
                                        resp_text = part.text
                                        break
                    if not resp_text:
                        logger.warning("VLM response blocked/empty for page %d", page_idx + 1)
                        return None

                if resp_text:
                    result = _parse_vlm_response(resp_text)
                    if result:
                        logger.info(
                            "  VLM Page %d: type=%-18s confidence=%-6s (rule=%s) — %s",
                            page_idx + 1,
                            result["vlm_page_type"],
                            result["vlm_confidence"],
                            rule_type,
                            result["vlm_reasoning"],
                        )
                        return result
                    else:
                        # JSON parse failed — log and retry
                        if attempt < _VLM_MAX_RETRIES:
                            logger.warning(
                                "VLM page %d: JSON parse failed (attempt %d/%d), retrying...",
                                page_idx + 1, attempt + 1, _VLM_MAX_RETRIES + 1,
                            )
                            _time.sleep(2)  # brief pause before retry
                            continue
                        else:
                            logger.warning(
                                "VLM page %d: JSON parse failed after %d attempts. Raw: %s",
                                page_idx + 1, _VLM_MAX_RETRIES + 1, repr(resp_text[:500]),
                            )
                            return None
                else:
                    logger.warning("VLM returned empty response for page %d", page_idx + 1)
                    return None

            except Exception as exc:
                if attempt < _VLM_MAX_RETRIES:
                    logger.warning("VLM attempt %d failed for page %d: %s", attempt + 1, page_idx + 1, exc)
                    _time.sleep(2)
                else:
                    raise

        return None

    except Exception as exc:
        logger.error("VLM call failed for page %d: %s", page_idx + 1, exc)
        return None


def vlm_verify_all_pages(
    pdf_path: str,
    total_pages: int,
    rule_results: Dict[int, Dict[str, Any]],
    page_image_paths: Optional[Dict[int, str]] = None,
    pages_to_verify: Optional[set] = None,
) -> Dict[int, Dict[str, Any]]:
    """
    Run VLM verification on pages and merge results with rule-based results.

    Uses ThreadPoolExecutor to verify multiple pages concurrently, dramatically
    reducing total verification time for multi-page documents.

    Args:
        pdf_path:         Absolute path to the PDF.
        total_pages:      Number of pages.
        rule_results:     {page_num: {"page_type": str, "is_relevant": bool}}
        page_image_paths: Optional {page_num: image_path} for pre-rendered images.
        pages_to_verify:  Optional set of page numbers to verify. If None, all
                          pages are verified (backward compatible).

    Returns:
        Updated rule_results dict with additional VLM fields:
            vlm_page_type, vlm_confidence, vlm_agrees
    """
    if not is_vlm_available():
        logger.info("VLM not available — skipping verification")
        for pn in rule_results:
            rule_results[pn]["vlm_page_type"] = None
            rule_results[pn]["vlm_confidence"] = None
            rule_results[pn]["vlm_agrees"] = None
        return rule_results

    from config import VLM_MAX_CONCURRENT
    pages_to_check = total_pages
    if pages_to_verify is not None:
        pages_to_check = len(pages_to_verify)
    logger.info("=== VLM verification started (%d pages to check, %d total, %d concurrent workers) ===",
                pages_to_check, total_pages, VLM_MAX_CONCURRENT)

    # Separate pages into those needing VLM and those that don't
    pages_skip = []
    pages_verify = []
    for pn in range(1, total_pages + 1):
        if pages_to_verify is not None and pn not in pages_to_verify:
            rule_results[pn].setdefault("vlm_page_type", None)
            rule_results[pn].setdefault("vlm_confidence", None)
            rule_results[pn].setdefault("vlm_agrees", None)
            pages_skip.append(pn)
        else:
            pages_verify.append(pn)

    def _verify_single_page(pn: int) -> tuple:
        """Verify one page via VLM — designed for parallel execution."""
        rule_info = rule_results.get(pn, {})
        rule_type = rule_info.get("page_type", "OTHER")
        img_path = (page_image_paths or {}).get(pn)

        vlm_result = vlm_classify_page(
            pdf_path=pdf_path,
            page_idx=pn - 1,
            rule_type=rule_type,
            existing_image_path=img_path,
        )
        return pn, vlm_result

    # Run VLM verification concurrently
    with ThreadPoolExecutor(max_workers=VLM_MAX_CONCURRENT) as executor:
        futures = {
            executor.submit(_verify_single_page, pn): pn
            for pn in pages_verify
        }
        for future in as_completed(futures):
            pn = futures[future]
            try:
                _, vlm_result = future.result()
            except Exception as exc:
                logger.error("VLM verification failed for page %d: %s", pn, exc)
                vlm_result = None

            if vlm_result:
                vlm_type = vlm_result["vlm_page_type"]
                vlm_conf = vlm_result["vlm_confidence"]
                agrees = vlm_type == rule_results[pn].get("page_type", "OTHER")

                rule_results[pn]["vlm_page_type"] = vlm_type
                rule_results[pn]["vlm_confidence"] = vlm_conf
                rule_results[pn]["vlm_agrees"] = agrees

                # ── VLM Override Logic ────────────────────────────────
                # When VLM disagrees with high confidence, it overrides
                # the rule-based classification.
                if not agrees and vlm_conf == "high":
                    old_type = rule_results[pn]["page_type"]
                    rule_results[pn]["page_type"] = vlm_type
                    rule_results[pn]["is_relevant"] = vlm_type in (
                        "LIGHTING_PLAN", "SCHEDULE", "SYMBOLS_LEGEND", "COVER"
                    )
                    rule_results[pn]["confidence_source"] = "vlm_override"
                    logger.info(
                        "  VLM OVERRIDE page %d: %s → %s (high confidence)",
                        pn, old_type, vlm_type,
                    )
                elif agrees and vlm_type == "SCHEDULE" and not rule_results[pn].get("is_relevant"):
                    rule_results[pn]["is_relevant"] = True
                    logger.info(
                        "  VLM confirmed SCHEDULE page %d is relevant", pn,
                    )
            else:
                rule_results[pn]["vlm_page_type"] = None
                rule_results[pn]["vlm_confidence"] = None
                rule_results[pn]["vlm_agrees"] = None

    agrees = sum(1 for pn in rule_results if rule_results[pn].get("vlm_agrees") is True)
    disagrees = sum(1 for pn in rule_results if rule_results[pn].get("vlm_agrees") is False)
    skipped = sum(1 for pn in rule_results if rule_results[pn].get("vlm_agrees") is None)

    logger.info(
        "=== VLM verification complete: %d agree, %d disagree, %d skipped ===",
        agrees, disagrees, skipped,
    )

    overridden = sum(
        1 for pn in rule_results
        if rule_results[pn].get("confidence_source") == "vlm_override"
    )
    if overridden:
        logger.info("  VLM overrode %d page classification(s)", overridden)

    return rule_results


# ══════════════════════════════════════════════════════════════════════════════
#  Scanned Page Detection
# ══════════════════════════════════════════════════════════════════════════════

_SCANNED_TEXT_THRESHOLD = 50  # chars — below this, page is likely scanned


def detect_scanned_pages(page_results: list) -> list:
    """
    Detect pages that appear to be scanned (no extractable text).

    Heuristics:
        1. text_length < 50 chars  → almost certainly scanned/rasterised.
        2. ocr_used is True        → text came from OCR, not native PDF text.
        3. Page classified as SCHEDULE but text_length < 200 → schedule is
           likely an image (normal schedule pages have 500+ chars from the
           table cells).  This catches partially-scanned pages where OCR
           missed the table.

    Returns:
        List of 1-based page numbers that are likely scanned.
    """
    scanned = []
    for pr in page_results:
        text_len = pr.get("text_length", 0) or 0
        ocr_used = pr.get("ocr_used", False)
        page_type = pr.get("page_type", "")

        if text_len < _SCANNED_TEXT_THRESHOLD:
            scanned.append(pr["page_number"])
        elif ocr_used:
            scanned.append(pr["page_number"])
        elif page_type == "SCHEDULE" and text_len < 200:
            # Schedule pages normally have dense table text; if very little
            # text was extracted, the schedule is probably rasterised.
            scanned.append(pr["page_number"])
    return scanned


# ══════════════════════════════════════════════════════════════════════════════
#  VLM Table Verification
# ══════════════════════════════════════════════════════════════════════════════

_TABLE_VERIFY_PROMPT = """You are an expert electrical engineer reviewing a construction drawing page.

This page has been identified as containing a schedule/table.

Analyse the attached page image and determine:

1. Does this page contain a LIGHT FIXTURE SCHEDULE (also called "Lighting Fixture Schedule" or "Luminaire Schedule")?
   - A Light Fixture Schedule lists lighting fixture TYPES (short codes like A, B, C, D, AL1, WL2, EX1, EM) with columns for description, voltage, mounting, wattage, etc.
   - It does NOT list circuit numbers, breaker sizes, or panel loads.
   - A page may have multiple tables — look for ANY table that is a Light Fixture Schedule.

2. What other tables are present?

IMPORTANT: The following are NOT Light Fixture Schedules:
- Panel Schedule (lists circuits, breakers, loads)
- Motor Schedule (lists motors, HP, RPM, FLA)
- Equipment Schedule (lists equipment items)
- Lighting Control Panel Schedule (lists zones, control types)
- Switchboard Schedule, Distribution Schedule

If the page has BOTH a Motor Schedule and a Light Fixture Schedule, answer YES (has_fixture_schedule: true).

Respond with ONLY valid JSON (no markdown, no commentary):
{"has_fixture_schedule": true, "fixture_schedule_confidence": "high", "table_types_found": ["Light Fixture Schedule"], "reasoning": "Page shows a table titled Light Fixture Schedule with fixture types A through F"}
"""


def vlm_verify_table(
    pdf_path: str,
    page_number: int,
    table_rows: list = None,
) -> Optional[Dict]:
    """
    Use VLM to verify if a page contains a Light Fixture Schedule.

    Args:
        pdf_path: Path to the PDF file.
        page_number: 1-based page number.
        table_rows: Optional table rows for context (not sent to VLM).

    Returns:
        {has_fixture_schedule: bool, confidence: str, table_types: list}
        or None if VLM is unavailable.
    """
    client = _get_model()
    if client is None:
        return None

    img_bytes = _render_page_image(pdf_path, page_number - 1)
    if img_bytes is None:
        return None

    try:
        import google.generativeai as genai
        from PIL import Image

        pil_image = Image.open(io.BytesIO(img_bytes))
        pil_image.load()
        img_bytes = None

        generation_config = genai.GenerationConfig(temperature=0.1, max_output_tokens=512)
        response = client.generate_content(
            [pil_image, _TABLE_VERIFY_PROMPT],
            generation_config=generation_config,
            request_options={"timeout": _VLM_TIMEOUT},
        )

        if response and response.text:
            data = _robust_json_parse(response.text)
            if data is None:
                logger.warning("VLM table verify page %d: unparseable response", page_number)
                return None

            # If has_fixture_schedule is missing from the data (e.g. truncated JSON),
            # return None so the caller does NOT override Docling's detection.
            if "has_fixture_schedule" not in data:
                logger.warning(
                    "VLM table verify page %d: response missing 'has_fixture_schedule' key — skipping",
                    page_number,
                )
                return None

            result = {
                "has_fixture_schedule": bool(data.get("has_fixture_schedule", False)),
                "confidence": data.get("fixture_schedule_confidence", "low"),
                "table_types": data.get("table_types_found", []),
                "reasoning": data.get("reasoning", ""),
            }
            logger.info(
                "  VLM table verify page %d: fixture_schedule=%s (conf=%s) — %s",
                page_number, result["has_fixture_schedule"],
                result["confidence"], result["reasoning"],
            )
            return result
    except Exception as exc:
        logger.error("VLM table verification failed for page %d: %s", page_number, exc)
    return None


# ══════════════════════════════════════════════════════════════════════════════
#  VLM Table Extraction (Gemini Fallback)
# ══════════════════════════════════════════════════════════════════════════════

_TABLE_EXTRACT_PROMPT = """You are an expert electrical engineer extracting data from a construction drawing.

Look at this page image and find the LIGHT FIXTURE SCHEDULE table.
Accepted table titles (any of these count):
  "Light Fixture Schedule", "Lighting Fixture Schedule", "Luminaire Schedule",
  "Fixture Schedule", "Fixture Type Schedule", "Lighting Schedule",
  "Electrical Fixture Schedule", "LED Fixture Schedule".

IMPORTANT: This page may contain MULTIPLE tables. Find the Light Fixture Schedule ONLY.
DO NOT extract from:
- Motor Schedule (lists motors, HP, RPM, FLA, MCA)
- Panel Schedule / Branch Panel (lists circuits, breaker sizes, loads in amps)
- Lighting Control Panel Schedule (lists zones, control types)
- Equipment Schedule or Switchboard Schedule

FIXTURE CODE — CRITICAL:
The "code" field is the SHORT TYPE DESIGNATION printed in the first column (titled TYPE,
MARK, SYMBOL, ID, or FIXTURE ID). Copy it EXACTLY as printed — every letter AND digit.
  CORRECT: "A1", "A1R", "B1", "EX1", "CV1", "RWH1", "G2"
  WRONG:   "A" when the table shows "A1" — do NOT drop trailing digits
It is 1–6 characters total (e.g. A, A1, A1R, B, B1, EX1, EX2, RWH1, CV1, G2).
It is NEVER a catalog/model number. If a value has many hyphens or is >10 chars,
put it in "description" and use the short code from the type/mark column.

EXTRACTION RULES:
1. Extract EVERY row — do NOT truncate after the first 20-30 rows.
2. Merged/multi-line cells: concatenate with a single space.
3. Continuation rows (blank code cell): append text to the previous fixture.
4. Skip header/sub-header rows and blank rows.

FIELDS (empty string "" when column absent):
- "code": EXACT type designation copied from the table (1–6 chars, e.g. A1, A1R, EX1).
- "description": Full fixture description.
- "fixture_style": Form factor (e.g. RECESSED TROFFER, PENDANT). "" if absent.
- "voltage": Voltage rating (e.g. 120V, 277V, 120/277V).
- "mounting": Mounting type (e.g. RECESSED, SURFACE, PENDANT, WALL).
- "lumens": Lumen output (e.g. 3500, 4000 LUM).
- "cct": Color temperature (e.g. 3000K, 4000K).
- "dimming": Dimming/driver info (e.g. 0-10V, DALI, NON-DIM).
- "max_va": Wattage or VA (e.g. 35W, 45VA).

Return ONLY valid JSON (no markdown fences):
{"table_type": "light_fixture_schedule", "fixtures": [{"code": "A1", "description": "2x4 LED TROFFER", "fixture_style": "RECESSED", "voltage": "120/277V", "mounting": "RECESSED", "lumens": "5000", "cct": "4000K", "dimming": "0-10V", "max_va": "40W"}]}

If no Light Fixture Schedule is found on this page:
{"table_type": "none", "fixtures": []}
"""


def vlm_extract_table(
    pdf_path: str,
    page_number: int,
) -> Optional[Dict]:
    """
    Use Gemini VLM to extract table data from a page image.
    Fallback for when Docling extraction fails or returns empty results.

    Uses 250 DPI for sharper cell text on scanned documents.

    Args:
        pdf_path: Path to the PDF file.
        page_number: 1-based page number.

    Returns:
        {"table_type": str, "fixtures": list[dict]}
        or None if extraction fails or no fixture schedule found.
    """
    client = _get_model()
    if client is None:
        return None

    img_bytes = _render_page_image(pdf_path, page_number - 1, dpi=200, max_dim=_VLM_EXTRACT_MAX_DIM)
    if img_bytes is None:
        return None

    import time as _time
    try:
        import google.generativeai as genai
        from PIL import Image

        pil_image = Image.open(io.BytesIO(img_bytes))
        pil_image.load()
        img_bytes = None

        generation_config = genai.GenerationConfig(
            temperature=0.0,
            max_output_tokens=_VLM_EXTRACT_MAX_TOKENS,
            response_mime_type="application/json",
        )
        
        def _call_gemini():
            return client.generate_content(
                [pil_image, _TABLE_EXTRACT_PROMPT],
                generation_config=generation_config,
                request_options={"timeout": _VLM_EXTRACT_TIMEOUT},
            )

        # Retry loop: attempt up to _VLM_MAX_RETRIES + 1 times on JSON parse failure
        for attempt in range(_VLM_MAX_RETRIES + 1):
            try:
                with ThreadPoolExecutor(max_workers=1) as pool:
                    future = pool.submit(_call_gemini)
                    try:
                        response = future.result(timeout=_VLM_EXTRACT_TIMEOUT + 15)
                    except FuturesTimeoutError:
                        logger.error(
                            "VLM table extraction timed out for page %d (>%ds)",
                            page_number, _VLM_EXTRACT_TIMEOUT,
                        )
                        return None

                resp_text = None
                try:
                    resp_text = response.text
                except (ValueError, AttributeError):
                    if hasattr(response, 'candidates') and response.candidates:
                        for candidate in response.candidates:
                            if hasattr(candidate, 'content') and candidate.content:
                                for part in candidate.content.parts:
                                    if hasattr(part, 'text') and part.text:
                                        resp_text = part.text
                                        break

                if resp_text:
                    data = _robust_json_parse(resp_text)
                    if data:
                        table_type = data.get("table_type", "none")
                        fixtures = data.get("fixtures", [])

                        # Sometimes the model returns a direct list instead of wrapping it 
                        if isinstance(data, list):
                            fixtures = data
                            table_type = "light_fixture_schedule" if fixtures else "none"

                        if table_type == "none" or not fixtures:
                            logger.info("  VLM table extract page %d: no fixture schedule found", page_number)
                            return None

                        logger.info(
                            "  VLM table extract page %d: %s — %d fixtures",
                            page_number, table_type, len(fixtures),
                        )
                        return {
                            "table_type": table_type,
                            "fixtures": fixtures,
                        }
                    else:
                        if attempt < _VLM_MAX_RETRIES:
                            logger.warning(
                                "VLM table extract page %d: JSON parse failed (attempt %d/%d), retrying...",
                                page_number, attempt + 1, _VLM_MAX_RETRIES + 1,
                            )
                            import time as _time
                            _time.sleep(2)
                            continue
                        else:
                            logger.warning(
                                "VLM table extract page %d: unparseable response after %d attempts", 
                                page_number, _VLM_MAX_RETRIES + 1
                            )
                            return None
            except Exception as exc:
                if attempt < _VLM_MAX_RETRIES:
                    logger.warning("VLM table extract attempt %d failed for page %d: %s", attempt + 1, page_number, exc)
                    import time as _time
                    _time.sleep(2)
                else:
                    logger.error("VLM table extraction failed for page %d: %s", page_number, exc)
                    return None

    except Exception as exc:
        logger.error("VLM table extraction outer error for page %d: %s", page_number, exc)
    return None


def vlm_extract_fixtures(pdf_path: str, page_number: int) -> list:
    """
    High-level helper: extract structured fixture records via VLM.

    Returns a list of dicts matching FixtureRecord fields:
        code, description, fixture_style, voltage, mounting,
        lumens, cct, dimming, max_va

    Empty list on failure or if no fixture schedule is found.
    """
    result = vlm_extract_table(pdf_path, page_number)
    if result is None:
        return []

    _FIXTURE_FIELDS = ("code", "description", "fixture_style", "voltage",
                       "mounting", "lumens", "cct", "dimming", "max_va")

    fixtures = []
    for raw in result.get("fixtures", []):
        # Normalise: ensure all FixtureRecord fields exist, strip whitespace
        rec = {}
        for field in _FIXTURE_FIELDS:
            val = raw.get(field, "")
            rec[field] = str(val).strip() if val else ""
        # Skip rows with no code (blank/header rows the VLM may include)
        if not rec["code"]:
            continue
        fixtures.append(rec)

    logger.info("  VLM extracted %d structured fixtures from page %d", len(fixtures), page_number)
    return fixtures
