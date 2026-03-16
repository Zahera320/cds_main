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

    def extract_schedule_csv_string(self):
        """
        State machine to read the text file, isolate table blocks, and check
        if they belong to the lighting schedule based on surrounding text or content.

        Returns the best matching fixture schedule table, preferring tables with
        actual fixture column headers over sheet index tables.
        """
        if not self.input_txt.exists():
            raise FileNotFoundError(f"Input text file not found: {self.input_txt}")

        with open(self.input_txt, 'r', encoding='utf-8') as f:
            lines = f.readlines()

        state = "TEXT"
        recent_text = []
        current_table = []
        # Collect all candidates — pick the best one at the end
        candidates = []

        for line in lines:
            clean_line = line.strip()

            if state == "TEXT":
                # Look for the start of a table block
                if clean_line.startswith("TABLE ") and "rows" in clean_line.lower():
                    state = "EXPECT_TABLE_START_SEP"
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
                        })

                    # Reset and keep looking
                    state = "TEXT"
                    recent_text = []
                else:
                    # Append the CSV row to our current table buffer
                    current_table.append(clean_line)

        if not candidates:
            return None

        # Prefer tables that look like actual fixture schedules
        fixture_tables = [c for c in candidates if c["is_fixture"] and not c["is_index"]]
        if fixture_tables:
            # Pick the one with the most rows
            best = max(fixture_tables, key=lambda c: c["row_count"])
            return best["table"]

        # Fallback: pick any non-index table
        non_index = [c for c in candidates if not c["is_index"]]
        if non_index:
            best = max(non_index, key=lambda c: c["row_count"])
            return best["table"]

        # Last resort: use the largest candidate
        best = max(candidates, key=lambda c: c["row_count"])
        return best["table"]

    def create_schedule_csv(self):
        """
        Executes the extraction and writes the resulting string to a file.
        """
        logger.info(
            "Scanning %s for Lighting Schedule patterns...",
            self.input_txt.name,
        )
        csv_content = self.extract_schedule_csv_string()

        if csv_content:
            output_csv_path = self.output_dir / "lighting_schedule.csv"
            with open(output_csv_path, 'w', encoding='utf-8') as f:
                f.write(csv_content)

            logger.info(
                "Lighting Schedule extracted to %s", output_csv_path
            )
            return str(output_csv_path)
        else:
            logger.warning(
                "Could not find any table matching the Lighting Schedule patterns."
            )
            return None
