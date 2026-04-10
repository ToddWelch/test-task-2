"""
CSV loader for the customer data pipeline.

Reads a CSV file using Python's csv.DictReader, strips whitespace
from all fields, and converts NULL sentinel values to None.
Returns a list of RawCustomerRecord objects with 1-indexed row numbers.
"""

import csv
from pathlib import Path
from typing import Optional

from src.constants import NULL_SENTINEL_VALUES
from src.models import RawCustomerRecord


def _strip_and_nullify(value: str) -> Optional[str]:
    """
    Strip whitespace from a value and convert NULL sentinels to None.

    Returns a tuple of (cleaned_value, had_whitespace, was_null_sentinel).
    The caller can use these flags to log CLEANED issues.
    """
    stripped = value.strip()
    if stripped.lower() in NULL_SENTINEL_VALUES:
        return None
    return stripped


def _detect_whitespace(value: str) -> bool:
    """Return True if the value has leading or trailing whitespace."""
    return value != value.strip()


def _is_null_sentinel(value: str) -> bool:
    """Return True if the stripped value matches a NULL sentinel."""
    stripped = value.strip()
    return stripped.lower() in NULL_SENTINEL_VALUES and stripped != ""


def load_csv(file_path: str) -> tuple[list[RawCustomerRecord], list[dict]]:
    """
    Load a CSV file and return a list of RawCustomerRecord objects.

    Also returns a list of metadata dicts for each row, containing info
    about which fields had whitespace or null sentinels (for CLEANED logging).

    Row numbers are 1-indexed (not counting the header row).
    Row 1 = first data row after the header.

    Args:
        file_path: Path to the CSV file.

    Returns:
        Tuple of (records, row_metadata).
        row_metadata is a list of dicts, one per row, each with:
          - "whitespace_fields": list of field names that had whitespace
          - "null_sentinel_fields": list of (field_name, original_value) tuples
    """
    path = Path(file_path)
    if not path.exists():
        raise FileNotFoundError(f"Input file not found: {file_path}")

    records: list[RawCustomerRecord] = []
    row_metadata: list[dict] = []

    with open(path, "r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)

        for idx, row in enumerate(reader, start=1):
            whitespace_fields: list[str] = []
            null_sentinel_fields: list[tuple[str, str]] = []
            cleaned_row: dict[str, Optional[str]] = {}

            for key, value in row.items():
                if value is None:
                    cleaned_row[key] = None
                    continue

                # Track whitespace
                if _detect_whitespace(value):
                    whitespace_fields.append(key)

                # Track null sentinels (before stripping, but after checking)
                if _is_null_sentinel(value):
                    null_sentinel_fields.append((key, value.strip()))

                # Apply strip and nullify
                cleaned_row[key] = _strip_and_nullify(value)

            cleaned_row["row_number"] = idx

            record = RawCustomerRecord(**cleaned_row)
            records.append(record)
            row_metadata.append({
                "whitespace_fields": whitespace_fields,
                "null_sentinel_fields": null_sentinel_fields,
            })

    return records, row_metadata
