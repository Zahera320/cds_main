"""
Key Notes Extractor (VLM-Only)
===============================
Extracts KEY NOTES from the top-left corner of Lightning (LIGHTING_PLAN) pages
using Gemini Vision Language Model exclusively.

Public API:
    extract_keynotes_vlm(pages) → {page_number: [note_dict, ...]}
"""

import io
import json
import os
import logging
from typing import List, Dict

logger = logging.getLogger(__name__)

# ── Gemini VLM keynote extraction prompt ───────────────────────────────
_VLM_KEYNOTE_PROMPT = """You are an expert at reading electrical engineering construction drawings.

Look at the TOP-LEFT CORNER of this lighting plan drawing page and find the KEY NOTES section (may also be labeled "KEYED NOTES").

Extract ALL key notes from the KEY NOTES section. Each note has a number (or letter) and a description.

Return the result as a JSON object with this exact format:
{
  "found": true,
  "notes": [
    {"number": "1", "text": "FULL TEXT OF NOTE 1"},
    {"number": "2", "text": "FULL TEXT OF NOTE 2"}
  ]
}

If there is NO KEY NOTES section on this page, return:
{"found": false, "notes": []}

Rules:
- Only extract from the KEY NOTES or KEYED NOTES section in the top-left corner
- Do NOT extract from GENERAL NOTES, ELECTRICAL NOTES, LIGHTING NOTES, or other sections
- Include the COMPLETE text of each note — do not truncate
- Preserve the original numbering (could be 1,2,3 or A,B,C or mixed)
- Return valid JSON only, no markdown fences or extra text
"""


def _vlm_extract_keynotes(image_path: str) -> List[Dict[str, str]]:
    """
    Use Gemini VLM to extract KEY NOTES from a page image.
    Returns list of {"number": str, "text": str, "section": "KEY NOTES"}.
    """
    try:
        from processing.vlm_classifier import _get_model, _load_existing_image
        import google.generativeai as genai
    except ImportError:
        logger.warning("Gemini VLM not available for keynote extraction")
        return []

    model = _get_model()
    if model is None:
        return []

    img_bytes = _load_existing_image(image_path)
    if not img_bytes:
        return []

    try:
        from PIL import Image as _PILImage
        pil_image = _PILImage.open(io.BytesIO(img_bytes))

        # Downsample large images
        w, h = pil_image.size
        max_dim = 2048
        if w > max_dim or h > max_dim:
            scale = max_dim / max(w, h)
            pil_image = pil_image.resize(
                (max(1, int(w * scale)), max(1, int(h * scale))),
                _PILImage.LANCZOS,
            )

        pil_image.load()

        generation_config = genai.GenerationConfig(
            temperature=0.0,
            max_output_tokens=8192,
            response_mime_type="application/json",
        )

        response = model.generate_content(
            [pil_image, _VLM_KEYNOTE_PROMPT],
            generation_config=generation_config,
            request_options={"timeout": 120},
        )

        result = json.loads(response.text)
        if not result.get("found") or not result.get("notes"):
            return []

        notes = []
        for n in result["notes"]:
            num = str(n.get("number", "")).strip()
            text = str(n.get("text", "")).strip()
            if num and text:
                notes.append({"number": num, "text": text, "section": "KEY NOTES"})

        logger.info("VLM keynote extraction: %s → %d notes",
                     os.path.basename(image_path), len(notes))
        return notes

    except Exception as exc:
        logger.warning("VLM keynote extraction failed for %s: %s", image_path, exc)
        return []


def extract_keynotes_vlm(
    pages: list,
) -> Dict[int, List[Dict[str, str]]]:
    """
    Extract key notes from LIGHTING_PLAN pages using VLM only.

    Args:
        pages: list of DocumentPage ORM objects with .page_type,
               .page_number, .image_path attributes.

    Returns:
        {page_number: [note_dict, ...]} — only pages with notes included.
    """
    result = {}

    for page in pages:
        page_type = getattr(page, 'page_type', '') or ''
        if page_type != 'LIGHTING_PLAN':
            continue

        image_path = getattr(page, 'image_path', None)
        if not image_path:
            logger.debug("No image_path for LIGHTING_PLAN page %d, skipping",
                         page.page_number)
            continue

        notes = _vlm_extract_keynotes(image_path)
        if notes:
            result[page.page_number] = notes
            logger.info("VLM keynotes: page %d → %d keynotes extracted",
                        page.page_number, len(notes))

    return result
