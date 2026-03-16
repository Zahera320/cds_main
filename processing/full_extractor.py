"""
Full Document Text + Table Extractor
======================================
Extracts all text and tables from every page of a PDF and writes them
into a single combined text file.

The output format is designed for downstream parsing by ScheduleIsolator:
  - Page headers: "=== PAGE N ==="
  - Regular text: direct text content
  - Table blocks: "TABLE N (M rows)" / "======" / CSV rows / "======"

Public API:
    DocumentExtractor(pdf_path, output_dir).extract_all() → str (path to output file)
"""

import csv
import io
import logging
import os
from pathlib import Path

from docling.document_converter import DocumentConverter, PdfFormatOption
from docling.datamodel.pipeline_options import PdfPipelineOptions
from docling.datamodel.base_models import InputFormat

logger = logging.getLogger(__name__)


class DocumentExtractor:
    """Extracts text and tables from all pages of a PDF document using Docling."""

    def __init__(self, pdf_path, output_dir):
        self.pdf_path = Path(pdf_path)
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def extract_all(self):
        """
        Extract text and tables from every page.

        Returns the path to the combined output text file.
        """
        if not self.pdf_path.exists():
            raise FileNotFoundError(f"PDF not found: {self.pdf_path}")

        output_path = self.output_dir / f"{self.pdf_path.stem}_text_table.txt"

        # Convert with Docling
        pipeline_opts = PdfPipelineOptions(
            do_table_structure=True,
            do_ocr=True,
        )
        converter = DocumentConverter(
            allowed_formats=[InputFormat.PDF],
            format_options={
                InputFormat.PDF: PdfFormatOption(pipeline_options=pipeline_opts),
            },
        )
        conv_result = converter.convert(str(self.pdf_path))
        doc = conv_result.document
        num_pages = doc.num_pages()

        # Group text items by page number
        text_by_page: dict = {}
        for text_item in doc.texts:
            if text_item.prov:
                page_no = text_item.prov[0].page_no
            else:
                page_no = 0
            text_by_page.setdefault(page_no, []).append(text_item.text)

        # Group tables by page number
        tables_by_page: dict = {}
        for idx, table_item in enumerate(doc.tables, start=1):
            page_no = table_item.prov[0].page_no if table_item.prov else 0
            tables_by_page.setdefault(page_no, []).append((idx, table_item))

        table_counter = 0

        with open(output_path, 'w', encoding='utf-8') as f:
            for page_num in range(1, num_pages + 1):
                f.write(f"\n{'=' * 60}\n")
                f.write(f"=== PAGE {page_num} ===\n")
                f.write(f"{'=' * 60}\n\n")

                # Write text items for this page
                texts = text_by_page.get(page_num, [])
                if texts:
                    f.write("\n".join(texts))
                    f.write("\n\n")

                # Write tables for this page
                for _, table_item in tables_by_page.get(page_num, []):
                    try:
                        df = table_item.export_to_dataframe(doc)
                        rows = [df.columns.tolist()] + df.values.tolist()
                        rows = [
                            [(str(c) if c is not None else "") for c in row]
                            for row in rows
                        ]
                        if not rows:
                            continue

                        table_counter += 1
                        f.write(f"\nTABLE {table_counter} ({len(rows)} rows)\n")
                        f.write("=" * 60 + "\n")

                        buf = io.StringIO()
                        writer = csv.writer(buf)
                        for row in rows:
                            writer.writerow(row)
                        f.write(buf.getvalue())

                        f.write("=" * 60 + "\n\n")
                    except Exception as exc:
                        logger.warning(
                            "Table extraction failed for page %d: %s",
                            page_num, exc,
                        )

                f.write(f"\n{'─' * 40}\n")

        logger.info(
            "Combined text+table extraction complete: %s (%d tables found)",
            output_path.name, table_counter,
        )
        return str(output_path)
