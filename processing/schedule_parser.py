"""
Schedule Parser — Lighting Schedule Isolator
=============================================
Parses the combined text/table output file from DocumentExtractor,
finds the table associated with any Lighting Schedule, and saves it as a CSV.

Uses a state machine to identify table blocks and matches them against
dynamic lighting schedule patterns. Validates candidates to prefer real
fixture schedule tables over sheet index tables that merely mention schedules.

Public API:
    ScheduleIsolator(input_txt_path, output_dir).create_schedule_csv() → str | None
"""

import os
import re
import logging
from pathlib import Path
from typing import Optional, List, Tuple

logger = logging.getLogger(__name__)

# Column headers that indicate a real fixture schedule table (general)
_FIXTURE_TABLE_KEYWORDS = [
    "type", "voltage", "mounting", "lamps", "ballast", "driver",
    "fixture", "lumens", "wattage", "va", "description", "catalog",
    "dimming", "cct", "color", "lens", "manufacturer",
]

# At least ONE of these stronger fixture-specific keywords must be present.
# This prevents generic tables (e.g. Lighting Control Panel) with only common
# words like "type" and "description" from being misidentified.
_STRONG_FIXTURE_KEYWORDS = [
    "lumens", "wattage", "lamps", "ballast", "driver", "dimming",
    "cct", "catalog", "lens", "mounting", "voltage", "manufacturer",
    "luminaire", "luminaires",
]

# Patterns that indicate this is just a sheet index / drawing list
_SHEET_INDEX_KEYWORDS = [
    "sheet name", "sheet number", "drawing number", "sheet total",
    "sheet index", "drawing index", "drawing list",
]

# Patterns that clearly indicate a non-fixture schedule (Lighting Control Panel etc.)
_CONTROL_TABLE_KEYWORDS = [
    "lighting control panel", "control zone", "override switch",
    "occupancy sensor", "time schedule", "daylight zone",
]


class ScheduleIsolator:
    """
    Parses the text/table output file from the DocumentExtractor, finds the
    table associated with any Lighting Schedule, and saves it as a CSV.
    """

    def __init__(self, input_txt_path, output_dir):
        self.input_txt = Path(input_txt_path)
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

        # DYNAMIC PATTERN: Matches "LIGHT" or "LIGHTING" + [any words] + "SCHEDULE"
        # Examples matched: "LIGHT FIXTURE SCHEDULE", "LIGHTING FIXTURE SCHEDULE"
        # Excludes: "LIGHTING CONTROL SCHEDULE", "LIGHTING CONTROL DEVICE SCHEDULE"
        self.target_pattern = re.compile(
            r"LIGHT(?:ING)?\s+(?!CONTROL)[\s\w]*SCHEDULE", re.IGNORECASE
        )

    @staticmethod
    def _is_fixture_table(table_string: str) -> bool:
        """Check if a table contains fixture-schedule column headers.

        Requires:
          - At least 2 general fixture keywords (broad check), AND
          - At least 1 strong fixture-specific keyword (no generic tables), AND
          - Not a Lighting Control Panel or similar control schedule.
        """
        table_lower = table_string[:1000].lower()
        # Reject control tables immediately
        if any(kw in table_lower for kw in _CONTROL_TABLE_KEYWORDS):
            return False
        general_matches = sum(1 for kw in _FIXTURE_TABLE_KEYWORDS if kw in table_lower)
        has_strong_keyword = any(kw in table_lower for kw in _STRONG_FIXTURE_KEYWORDS)
        return general_matches >= 2 and has_strong_keyword

    @staticmethod
    def _is_sheet_index(table_string: str) -> bool:
        """Check if a table looks like a sheet index / drawing list."""
        table_lower = table_string[:500].lower()
        return any(kw in table_lower for kw in _SHEET_INDEX_KEYWORDS)

    def extract_schedule_csv_string(self) -> Tuple[Optional[str], List[int]]:
        """
        State machine to read the text file, isolate table blocks, and check
        if they belong to the lighting schedule based on surrounding text or content.

        Returns a tuple of (csv_content, page_numbers) where:
        - csv_content: the best matching fixture schedule table content
        - page_numbers: list of page numbers where fixture schedules were found

        Prefers tables with actual fixture column headers over sheet index tables.
        """
        if not self.input_txt.exists():
            raise FileNotFoundError(f"Input text file not found: {self.input_txt}")

        with open(self.input_txt, 'r', encoding='utf-8') as f:
            lines = f.readlines()

        state = "TEXT"
        recent_text = []
        current_table = []
        current_page = 0  # Track current page from === PAGE N === markers
        table_start_page = 0  # Page where current table started
        # Collect all candidates — pick the best one at the end
        candidates = []

        # Pattern to match page markers: === PAGE N === or similar
        page_marker_pattern = re.compile(r'===\s*PAGE\s+(\d+)\s*===', re.IGNORECASE)

        for line in lines:
            clean_line = line.strip()

            # Track current page number from page markers
            page_match = page_marker_pattern.search(clean_line)
            if page_match:
                current_page = int(page_match.group(1))
                continue  # Don't process page markers as content

            if state == "TEXT":
                # Look for the start of a table block
                if clean_line.startswith("TABLE ") and "rows" in clean_line.lower():
                    state = "EXPECT_TABLE_START_SEP"
                    table_start_page = current_page  # Remember which page this table is on
                else:
                    # Keep track of recent non-formatting text to catch headers
                    if (
                        clean_line
                        and not clean_line.startswith("==")
                        and not clean_line.startswith("--")
                        and not clean_line.startswith("──")
                    ):
                        recent_text.append(clean_line)
                        # Keep a rolling window of the last 15 lines of text
                        if len(recent_text) > 15:
                            recent_text.pop(0)

            elif state == "EXPECT_TABLE_START_SEP":
                # The line immediately after "TABLE X..." should be "======"
                if clean_line.startswith("======="):
                    state = "IN_TABLE"
                    current_table = []

            elif state == "IN_TABLE":
                # The line "======" marks the end of the CSV table data
                if clean_line.startswith("======="):
                    table_string = "\n".join(current_table)
                    context_string = " ".join(recent_text)

                    # DYNAMIC CHECK: Does the regex match the context header
                    # OR the first few lines of the table?
                    search_area = context_string + " \n " + table_string[:500]

                    if self.target_pattern.search(search_area):
                        is_fixture = self._is_fixture_table(table_string)
                        is_index = self._is_sheet_index(table_string)
                        candidates.append({
                            "table": table_string,
                            "is_fixture": is_fixture,
                            "is_index": is_index,
                            "row_count": len(current_table),
                            "page_number": table_start_page,  # Track source page
                        })

                    # Reset and keep looking
                    state = "TEXT"
                    recent_text = []
                else:
                    # Append the CSV row to our current table buffer
                    current_table.append(clean_line)

        if not candidates:
            return None, []

        # Collect all page numbers where fixture schedules were found
        all_fixture_pages = [
            c["page_number"] for c in candidates
            if c["is_fixture"] and not c["is_index"] and c["page_number"] > 0
        ]

        # Prefer tables that look like actual fixture schedules
        fixture_tables = [c for c in candidates if c["is_fixture"] and not c["is_index"]]
        if fixture_tables:
            # Pick the one with the most rows
            best = max(fixture_tables, key=lambda c: c["row_count"])
            return best["table"], all_fixture_pages

        # Fallback: pick any non-index table
        non_index = [c for c in candidates if not c["is_index"]]
        if non_index:
            best = max(non_index, key=lambda c: c["row_count"])
            # Include page numbers from non-index candidates
            non_index_pages = [c["page_number"] for c in non_index if c["page_number"] > 0]
            return best["table"], non_index_pages

        # Last resort: use the largest candidate
        best = max(candidates, key=lambda c: c["row_count"])
        return best["table"], [best["page_number"]] if best["page_number"] > 0 else []

    def create_schedule_csv(self) -> Tuple[Optional[str], List[int]]:
        """
        Executes the extraction and writes the resulting string to a file.

        Returns a tuple of (csv_path, page_numbers) where:
        - csv_path: path to the saved CSV file, or None if no schedule found
        - page_numbers: list of page numbers where fixture schedules were found
        """
        logger.info(
            "Scanning %s for Lighting Schedule patterns...",
            self.input_txt.name,
        )
        csv_content, page_numbers = self.extract_schedule_csv_string()

        if csv_content:
            output_csv_path = self.output_dir / "lighting_schedule.csv"
            with open(output_csv_path, 'w', encoding='utf-8') as f:
                f.write(csv_content)

            logger.info(
                "Lighting Schedule extracted to %s (found on pages %s)",
                output_csv_path, page_numbers if page_numbers else "unknown"
            )
            return str(output_csv_path), page_numbers
        else:
            logger.warning(
                "Could not find any table matching the Lighting Schedule patterns."
            )
            return None, []
