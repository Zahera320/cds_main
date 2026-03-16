"""
Plan Splitter — Lighting Panel Page Extraction
================================================
Identifies lighting panel/plan pages and extracts them into a separate PDF.

Uses either:
  - Existing page classification results (if available)
  - Text-based pattern matching (fallback)

Public API:
    PlanSplitter(pdf_path, output_dir)
        .extract_panel_pages(classified_pages=None) → str | None
"""

import logging
import re
from pathlib import Path

import fitz  # PyMuPDF

logger = logging.getLogger(__name__)

# Patterns that indicate a lighting panel/plan page
_LIGHTING_PLAN_PATTERNS = [
    re.compile(r"lighting\s+plan", re.IGNORECASE),
    re.compile(r"lighting\s+layout", re.IGNORECASE),
    re.compile(r"electrical\s+lighting", re.IGNORECASE),
    re.compile(r"lighting\s+fixture\s+plan", re.IGNORECASE),
    re.compile(r"lighting\s+area", re.IGNORECASE),
    re.compile(r"plan\s*[-–—]\s*lighting", re.IGNORECASE),
    re.compile(r"level\b.*\blighting", re.IGNORECASE),
    re.compile(r"fixture\s+location", re.IGNORECASE),
]


class PlanSplitter:
    """Extracts lighting plan pages from a PDF into a separate file."""

    def __init__(self, pdf_path, output_dir):
        self.pdf_path = Path(pdf_path)
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        # Populated after extract_panel_pages():
        # {split_page_idx: sheet_label}  e.g. {0: "E1.5", 1: "E2.3"}
        self.page_sheet_labels: dict = {}

    def extract_panel_pages(self, classified_pages=None):
        """
        Extract lighting panel/plan pages into a separate PDF.

        Args:
            classified_pages: Optional dict
                {page_num: {"page_type": str, "is_relevant": bool, "sheet_code": str}}
                If provided, uses classification results.
                If None, uses text-based pattern matching.

        Returns:
            Path to the split PDF, or None if no matching pages found.
            Also populates self.page_sheet_labels: {split_page_idx: sheet_label}
        """
        if not self.pdf_path.exists():
            raise FileNotFoundError(f"PDF not found: {self.pdf_path}")

        doc = fitz.open(str(self.pdf_path))
        panel_page_indices = []  # list of (original_1based_page_num, sheet_label)

        try:
            if classified_pages:
                # Use classification results
                for page_num, info in sorted(classified_pages.items()):
                    page_type = info.get("page_type", "")
                    if page_type == "LIGHTING_PLAN":
                        sheet_code = info.get("sheet_code") or ""
                        label = sheet_code if sheet_code else f"Page_{page_num}"
                        panel_page_indices.append((page_num - 1, label))  # 0-based idx

                # If classification found no LIGHTING_PLAN pages, fall back to
                # text-based scan — the classifier may have misclassified pages.
                if not panel_page_indices:
                    logger.warning(
                        "No LIGHTING_PLAN pages in classifications for %s — "
                        "falling back to text-based scan",
                        self.pdf_path.name,
                    )
                    for page_idx in range(len(doc)):
                        page = doc[page_idx]
                        text = page.get_text("text")
                        for pattern in _LIGHTING_PLAN_PATTERNS:
                            if pattern.search(text):
                                label = f"Page_{page_idx + 1}"
                                panel_page_indices.append((page_idx, label))
                                break
            else:
                # Fallback: text-based pattern matching
                for page_idx in range(len(doc)):
                    page = doc[page_idx]
                    text = page.get_text("text")
                    for pattern in _LIGHTING_PLAN_PATTERNS:
                        if pattern.search(text):
                            label = f"Page_{page_idx + 1}"
                            panel_page_indices.append((page_idx, label))
                            break

            if not panel_page_indices:
                logger.warning(
                    "No lighting panel pages found in %s",
                    self.pdf_path.name,
                )
                return None

            # Build the sheet label map (split_page_idx → label)
            self.page_sheet_labels = {
                split_idx: label
                for split_idx, (_, label) in enumerate(panel_page_indices)
            }

            # Create new PDF with just the panel pages
            output_path = self.output_dir / "lighting_panel_plans.pdf"
            split_doc = fitz.open()

            for orig_page_idx, _ in panel_page_indices:
                split_doc.insert_pdf(doc, from_page=orig_page_idx, to_page=orig_page_idx)

            split_doc.save(str(output_path))
            split_doc.close()

            logger.info(
                "Split PDF created: %s (%d lighting plan pages) — labels: %s",
                output_path.name,
                len(panel_page_indices),
                list(self.page_sheet_labels.values()),
            )
            return str(output_path)

        finally:
            doc.close()
