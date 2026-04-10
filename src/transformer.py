"""
Transformer for the customer data pipeline.

Takes a RawCustomerRecord and its ValidationIssues, applies all CLEANED
transformations, and returns a CustomerRecord if no ERROR issues exist.
Returns None if the record has ERRORs.
"""

from typing import Optional

from src.models import (
    RawCustomerRecord,
    CustomerRecord,
    ValidationIssue,
    Severity,
)


def _parse_float(value: Optional[str]) -> Optional[float]:
    """Parse a string to float. Returns None if not parseable."""
    if value is None:
        return None
    try:
        return float(value)
    except (ValueError, TypeError):
        return None


def _parse_int(value: Optional[str]) -> Optional[int]:
    """Parse a string to int. Returns None if not parseable."""
    if value is None:
        return None
    try:
        return int(float(value))
    except (ValueError, TypeError):
        return None


def _parse_newsletter(value: Optional[str]) -> Optional[bool]:
    """
    Parse the newsletter opt-in value (already normalized by cleaning rules).
    Expected values after cleaning: 'True', 'False', or None.
    """
    if value is None:
        return None
    if value == "True":
        return True
    if value == "False":
        return False
    # Fallback for values that were not cleaned (edge case)
    lower = value.lower().strip()
    if lower in ("true", "1", "yes", "y", "t"):
        return True
    if lower in ("false", "0", "no", "n", "f"):
        return False
    return None


def transform_record(
    record: RawCustomerRecord,
    issues: list[ValidationIssue],
) -> Optional[CustomerRecord]:
    """
    Transform a raw record into a clean CustomerRecord.

    The record has already been through cleaning rules (in validators.py),
    so the RawCustomerRecord fields already contain normalized values.
    This function converts types and builds the final CustomerRecord.

    Args:
        record: The cleaned RawCustomerRecord (after validation).
        issues: All ValidationIssues for this record.

    Returns:
        A CustomerRecord if no ERROR issues exist, otherwise None.
    """
    # Check for errors; if any exist, do not produce a clean record
    has_errors = any(
        issue.severity == Severity.ERROR for issue in issues
    )
    if has_errors:
        return None

    # Build the CustomerRecord from the cleaned raw data
    try:
        customer = CustomerRecord(
            customer_id=record.customer_id or "",
            first_name=record.first_name,
            last_name=record.last_name,
            email=record.email or "",
            phone=record.phone,
            date_of_birth=record.date_of_birth,
            signup_date=record.signup_date or "",
            city=record.city,
            state=record.state,
            zip_code=record.zip_code,
            loyalty_tier=record.loyalty_tier,
            total_spend=_parse_float(record.total_spend),
            num_orders=_parse_int(record.num_orders),
            last_order_date=record.last_order_date,
            newsletter_opt_in=_parse_newsletter(record.newsletter_opt_in),
            preferred_contact=record.preferred_contact,
            notes=record.notes,
        )
        return customer
    except Exception as e:
        # Log the failure so it is visible in verbose/debug output.
        # This should not happen if validation rules are correct.
        import sys
        print(
            f"WARNING: Failed to build CustomerRecord for row "
            f"{record.row_number} ({record.customer_id}): {e}",
            file=sys.stderr,
        )
        return None
