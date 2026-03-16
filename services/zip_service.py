"""
ZIP Service
============
Extracts PDF files from a ZIP archive with path-traversal protection.

Responsibilities:
    - Open and validate a ZIP file
    - Safely extract only PDF members into a target directory
    - Guard against path-traversal attacks (ZipSlip)

Raises FastAPI HTTPException on invalid input so callers can let
exceptions propagate unchanged.
"""

import os
import zipfile
from typing import List

import fitz  # PyMuPDF
from fastapi import HTTPException, status


class ZipService:
    """Stateless ZIP-handling helpers."""

    # ── Private ───────────────────────────────────────────────────────────────

    @staticmethod
    def _safe_extract_member(
        zip_ref: zipfile.ZipFile, member: str, extract_to: str
    ) -> str:
        """
        Extract a single ZIP member, ensuring the resolved target stays
        inside *extract_to* (prevents ZipSlip / path-traversal).
        """
        target_path = os.path.realpath(os.path.join(extract_to, member))
        abs_extract = os.path.realpath(extract_to)

        if not target_path.startswith(abs_extract + os.sep) and target_path != abs_extract:
            raise ValueError(f"Path traversal detected: {member}")

        os.makedirs(os.path.dirname(target_path), exist_ok=True)

        # Stream the member in chunks instead of loading it all into memory;
        # a large entry inside a ZIP could OOM the process if read() is used.
        chunk_size = 4 * 1024 * 1024  # 4 MB
        with zip_ref.open(member) as src, open(target_path, "wb") as dst:
            while True:
                chunk = src.read(chunk_size)
                if not chunk:
                    break
                dst.write(chunk)

        return target_path

    # ── Public API ────────────────────────────────────────────────────────────

    @classmethod
    def extract_pdfs(cls, zip_path: str, extract_to: str) -> List[str]:
        """
        Open *zip_path*, extract every `*.pdf` member into *extract_to*,
        and return a list of saved file paths.

        Raises:
            HTTPException 400  — empty ZIP, corrupt ZIP, or ZIP has no PDFs
            HTTPException 500  — unexpected extraction failure
        """
        pdf_files: List[str] = []

        try:
            with zipfile.ZipFile(zip_path, "r") as zip_ref:
                if not zip_ref.namelist():
                    raise HTTPException(
                        status_code=status.HTTP_400_BAD_REQUEST,
                        detail="ZIP file is empty (contains no files)",
                    )

                for name in zip_ref.namelist():
                    if name.endswith("/") or not name.lower().endswith(".pdf"):
                        continue
                    try:
                        saved = cls._safe_extract_member(zip_ref, name, extract_to)
                        pdf_files.append(saved)
                    except (ValueError, Exception):
                        continue  # skip traversal attempts or corrupt entries

        except zipfile.BadZipFile:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Corrupt ZIP file",
            )
        except HTTPException:
            raise
        except Exception:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to extract ZIP file",
            )

        return pdf_files

    @staticmethod
    def merge_pdfs(pdf_paths: List[str], output_path: str) -> int:
        """
        Merge a list of PDF files into a single PDF saved at *output_path*.

        Returns the total page count of the merged document.

        Raises:
            HTTPException 400  — no valid PDFs could be merged
            HTTPException 500  — unexpected failure during merging
        """
        try:
            merged = fitz.open()
            pages_added = 0
            for pdf_path in pdf_paths:
                try:
                    src = fitz.open(pdf_path)
                    merged.insert_pdf(src)
                    pages_added += len(src)
                    src.close()
                except Exception:
                    continue  # skip unreadable PDFs, try the rest

            if pages_added == 0:
                merged.close()
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="None of the PDFs inside the ZIP could be read",
                )

            merged.save(output_path)
            merged.close()
            return pages_added

        except HTTPException:
            raise
        except Exception:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to merge PDFs from ZIP",
            )
