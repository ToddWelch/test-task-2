"""
Pydantic models for the customer data pipeline.

Four core models:
- RawCustomerRecord: raw CSV input before any validation or cleaning
- CustomerRecord: validated customer matching the SQLite schema
- ValidationIssue: a single validation finding (error, warning, or cleaning action)
- PipelineRun: metadata about a single pipeline execution
"""

from datetime import datetime
from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field


class Severity(str, Enum):
    """Severity levels for validation issues."""
    ERROR = "ERROR"
    WARNING = "WARNING"
    CLEANED = "CLEANED"


class RawCustomerRecord(BaseModel):
    """
    Represents a single row from the input CSV before any validation.

    All fields are Optional[str] because the raw CSV may have missing
    or malformed data in any column. The row_number field tracks the
    original position in the CSV for error reporting.
    """
    row_number: int
    customer_id: Optional[str] = None
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    email: Optional[str] = None
    phone: Optional[str] = None
    date_of_birth: Optional[str] = None
    signup_date: Optional[str] = None
    city: Optional[str] = None
    state: Optional[str] = None
    zip_code: Optional[str] = None
    loyalty_tier: Optional[str] = None
    total_spend: Optional[str] = None
    num_orders: Optional[str] = None
    last_order_date: Optional[str] = None
    newsletter_opt_in: Optional[str] = None
    preferred_contact: Optional[str] = None
    notes: Optional[str] = None


class CustomerRecord(BaseModel):
    """
    Represents a validated, clean customer record ready for SQLite insertion.

    Field types match the SQLite schema. All cleaning and normalization
    has already been applied. This model enforces the constraints that
    the database schema requires (e.g., email is required and non-null,
    signup_date is required and non-null).
    """
    customer_id: str
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    email: str                              # Required, NOT NULL
    phone: Optional[str] = None
    date_of_birth: Optional[str] = None     # ISO 8601 (YYYY-MM-DD) or None
    signup_date: str                        # ISO 8601 (YYYY-MM-DD), NOT NULL
    city: Optional[str] = None
    state: Optional[str] = None             # 2-letter uppercase abbreviation
    zip_code: Optional[str] = None          # 5-digit string
    loyalty_tier: Optional[str] = None      # Bronze/Silver/Gold/Platinum
    total_spend: Optional[float] = None
    num_orders: Optional[int] = None
    last_order_date: Optional[str] = None   # ISO 8601 (YYYY-MM-DD) or None
    newsletter_opt_in: Optional[bool] = None  # True, False, or None
    preferred_contact: Optional[str] = None   # email/phone/mail
    notes: Optional[str] = None


class ValidationIssue(BaseModel):
    """
    Represents a single validation finding for a customer record.

    Every issue is tied to a pipeline run (run_id) and a specific
    customer/row. The severity determines whether the record is
    rejected (ERROR), loaded with a warning (WARNING), or auto-corrected
    (CLEANED). For CLEANED issues, original_value and corrected_value
    capture what changed.
    """
    run_id: str
    customer_id: Optional[str] = None
    row_number: Optional[int] = None
    field: Optional[str] = None
    rule_id: str
    severity: Severity
    message: str
    original_value: Optional[str] = None
    corrected_value: Optional[str] = None


class PipelineRun(BaseModel):
    """
    Metadata about a single pipeline execution.

    The input_hash (SHA256 of the input file) supports idempotency
    verification: running the pipeline twice on the same file should
    produce identical outputs.
    """
    run_id: str
    run_timestamp: str                      # ISO 8601 datetime
    input_file: str                         # Path to the input CSV
    input_hash: str                         # SHA256 hex digest of input file
    total_records: int = 0
    clean_records: int = 0
    rejected_records: int = 0
    warning_records: int = 0
    cleaning_actions: int = 0
