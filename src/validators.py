"""
Validation rules for the customer data pipeline.

35 rules organized by severity:
- ERROR (rules 1-15): Record rejected, not loaded to SQLite
- WARNING (rules 16-28): Record loads but flagged in report
- CLEANED (rules 29-35): Auto-corrected, original value logged

The validate function runs cleaning rules FIRST (to normalize data),
then error rules, then warning rules. This way error checks run
on cleaned data.
"""

import re
from datetime import datetime, date
from typing import Optional

from src.constants import (
    STATE_ABBREVIATIONS,
    VALID_STATE_ABBREVS,
    LOYALTY_TIER_LOOKUP,
    VALID_LOYALTY_TIERS,
    TIER_SPEND_THRESHOLDS,
    AOV_OUTLIER_MIN,
    AOV_OUTLIER_MAX,
    EMAIL_PATTERN,
    DOUBLE_AT_PATTERN,
    LEADING_DOT_LOCAL,
    TRAILING_DOT_LOCAL,
    CONSECUTIVE_DOTS,
    PHONE_DIGITS_PATTERN,
    PHONE_7_DIGITS_PATTERN,
    ZIP_CODE_PATTERN,
    NAME_SUSPICIOUS_CHARS,
    CITY_HAS_DIGITS,
    DATE_SLASH_PATTERN,
    DATE_DASH_MDY_PATTERN,
    DATE_ISO_PATTERN,
    NEWSLETTER_TRUTHY,
    NEWSLETTER_FALSY,
    MIN_CUSTOMER_AGE,
    MAX_CUSTOMER_AGE,
)
from src.models import RawCustomerRecord, ValidationIssue, Severity


# Today's date for age and future-date checks
TODAY = date(2026, 4, 10)


# ---------------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------------

def _parse_date(value: Optional[str]) -> Optional[date]:
    """
    Parse a date string in ISO 8601 format (YYYY-MM-DD) to a date object.
    Returns None if the value is None or cannot be parsed.
    """
    if not value:
        return None
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except (ValueError, TypeError):
        return None


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


def _normalize_date(value: Optional[str]) -> Optional[str]:
    """
    Normalize date formats to ISO 8601 (YYYY-MM-DD).
    Handles: MM/DD/YYYY, MM-DD-YYYY, M/DD/YY, and already-ISO dates.
    Returns the normalized string, or the original if already ISO or None.
    """
    if not value:
        return value

    # Already ISO format
    if DATE_ISO_PATTERN.match(value):
        return value

    # MM/DD/YYYY or M/DD/YY format
    match = DATE_SLASH_PATTERN.match(value)
    if match:
        month, day, year = match.groups()
        year = int(year)
        if year < 100:
            # Two-digit year: 00-29 = 2000s, 30-99 = 1900s
            year = year + 2000 if year < 30 else year + 1900
        try:
            d = date(year, int(month), int(day))
            return d.isoformat()
        except ValueError:
            return value

    # MM-DD-YYYY or MM-DD-YY format
    match = DATE_DASH_MDY_PATTERN.match(value)
    if match:
        month, day, year = match.groups()
        year = int(year)
        if year < 100:
            year = year + 2000 if year < 30 else year + 1900
        try:
            d = date(year, int(month), int(day))
            return d.isoformat()
        except ValueError:
            return value

    return value


def _normalize_phone(value: Optional[str]) -> Optional[str]:
    """
    Normalize phone to (XXX) XXX-XXXX format.
    Returns the normalized phone, or the original if it cannot be normalized.
    """
    if not value:
        return value

    digits = re.sub(r"\D", "", value)

    # 10 digits, possibly with leading 1
    match = PHONE_DIGITS_PATTERN.match(digits)
    if match:
        d = match.group(1)
        return f"({d[:3]}) {d[3:6]}-{d[6:]}"

    # 7 digits (missing area code) -- leave as-is, will be warned
    if PHONE_7_DIGITS_PATTERN.match(digits):
        return value

    return value


def _normalize_state(value: Optional[str]) -> Optional[str]:
    """
    Normalize state to 2-letter uppercase abbreviation.
    Handles full names, abbreviations with periods, lowercase, etc.
    """
    if not value:
        return value

    cleaned = value.strip()

    # Remove periods (handles "ca.", "N.Y.", "N.J.")
    no_periods = cleaned.replace(".", "").strip()

    # Check if it is already a valid 2-letter abbreviation
    if len(no_periods) == 2 and no_periods.upper() in VALID_STATE_ABBREVS:
        return no_periods.upper()

    # Try full name lookup
    lookup = no_periods.lower()
    if lookup in STATE_ABBREVIATIONS:
        return STATE_ABBREVIATIONS[lookup]

    # Return uppercase of original if nothing matches
    return cleaned.upper() if len(cleaned) <= 2 else cleaned


def _normalize_loyalty_tier(value: Optional[str]) -> Optional[str]:
    """Normalize loyalty tier to canonical casing (e.g., 'gold' -> 'Gold')."""
    if not value:
        return value
    lookup = value.strip().lower()
    return LOYALTY_TIER_LOOKUP.get(lookup, value)


def _normalize_newsletter(value: Optional[str]) -> Optional[str]:
    """
    Normalize newsletter opt-in to 'True' or 'False' string.
    Returns None for null/empty values.
    """
    if not value:
        return None
    lower = value.strip().lower()
    if lower in NEWSLETTER_TRUTHY:
        return "True"
    if lower in NEWSLETTER_FALSY:
        return "False"
    return value


# ---------------------------------------------------------------------------
# CLEANED rules (29-35) -- run first to normalize data
# ---------------------------------------------------------------------------

def run_cleaning_rules(
    record: RawCustomerRecord,
    run_id: str,
    row_meta: dict,
) -> tuple[RawCustomerRecord, list[ValidationIssue]]:
    """
    Run all cleaning rules on a record. Returns a new (cleaned) record
    and a list of CLEANED ValidationIssue objects.

    Modifies the record in place for the cleaned fields, then returns it.
    """
    issues: list[ValidationIssue] = []
    data = record.model_dump()

    # Rule 29: Whitespace trimming (already done in loader, but log it)
    for field_name in row_meta.get("whitespace_fields", []):
        issues.append(ValidationIssue(
            run_id=run_id,
            customer_id=data.get("customer_id"),
            row_number=data["row_number"],
            field=field_name,
            rule_id="R29_WHITESPACE_TRIM",
            severity=Severity.CLEANED,
            message=f"Leading/trailing whitespace removed from {field_name}",
            original_value=f"(had whitespace)",
            corrected_value=data.get(field_name),
        ))

    # Rule 35: N/A values converted to NULL (already done in loader, log it)
    for field_name, original_val in row_meta.get("null_sentinel_fields", []):
        issues.append(ValidationIssue(
            run_id=run_id,
            customer_id=data.get("customer_id"),
            row_number=data["row_number"],
            field=field_name,
            rule_id="R35_NULL_SENTINEL",
            severity=Severity.CLEANED,
            message=f"NULL sentinel '{original_val}' converted to NULL",
            original_value=original_val,
            corrected_value=None,
        ))

    # Rule 30: State normalization
    if data.get("state"):
        original_state = data["state"]
        normalized_state = _normalize_state(original_state)
        if normalized_state != original_state:
            data["state"] = normalized_state
            issues.append(ValidationIssue(
                run_id=run_id,
                customer_id=data.get("customer_id"),
                row_number=data["row_number"],
                field="state",
                rule_id="R30_STATE_NORMALIZE",
                severity=Severity.CLEANED,
                message=f"State normalized from '{original_state}' to '{normalized_state}'",
                original_value=original_state,
                corrected_value=normalized_state,
            ))

    # Rule 31: Phone normalization
    if data.get("phone"):
        original_phone = data["phone"]
        normalized_phone = _normalize_phone(original_phone)
        if normalized_phone != original_phone:
            data["phone"] = normalized_phone
            issues.append(ValidationIssue(
                run_id=run_id,
                customer_id=data.get("customer_id"),
                row_number=data["row_number"],
                field="phone",
                rule_id="R31_PHONE_NORMALIZE",
                severity=Severity.CLEANED,
                message=f"Phone normalized to standard format",
                original_value=original_phone,
                corrected_value=normalized_phone,
            ))

    # Zip code normalization: strip non-digit characters (e.g., "123-45" -> "12345")
    if data.get("zip_code"):
        original_zip = data["zip_code"]
        cleaned_zip = re.sub(r"\D", "", original_zip)
        if cleaned_zip != original_zip and ZIP_CODE_PATTERN.match(cleaned_zip):
            data["zip_code"] = cleaned_zip
            issues.append(ValidationIssue(
                run_id=run_id,
                customer_id=data.get("customer_id"),
                row_number=data["row_number"],
                field="zip_code",
                rule_id="R29_WHITESPACE_TRIM",
                severity=Severity.CLEANED,
                message=f"Zip code cleaned from '{original_zip}' to '{cleaned_zip}'",
                original_value=original_zip,
                corrected_value=cleaned_zip,
            ))

    # Rule 32: Loyalty tier case normalization
    if data.get("loyalty_tier"):
        original_tier = data["loyalty_tier"]
        normalized_tier = _normalize_loyalty_tier(original_tier)
        if normalized_tier != original_tier:
            data["loyalty_tier"] = normalized_tier
            issues.append(ValidationIssue(
                run_id=run_id,
                customer_id=data.get("customer_id"),
                row_number=data["row_number"],
                field="loyalty_tier",
                rule_id="R32_TIER_NORMALIZE",
                severity=Severity.CLEANED,
                message=f"Loyalty tier normalized from '{original_tier}' to '{normalized_tier}'",
                original_value=original_tier,
                corrected_value=normalized_tier,
            ))

    # Rule 33: Newsletter boolean normalization
    if data.get("newsletter_opt_in") is not None:
        original_newsletter = data["newsletter_opt_in"]
        normalized_newsletter = _normalize_newsletter(original_newsletter)
        if normalized_newsletter != original_newsletter:
            data["newsletter_opt_in"] = normalized_newsletter
            issues.append(ValidationIssue(
                run_id=run_id,
                customer_id=data.get("customer_id"),
                row_number=data["row_number"],
                field="newsletter_opt_in",
                rule_id="R33_NEWSLETTER_NORMALIZE",
                severity=Severity.CLEANED,
                message=f"Newsletter opt-in normalized from '{original_newsletter}' to '{normalized_newsletter}'",
                original_value=original_newsletter,
                corrected_value=normalized_newsletter,
            ))

    # Rule 34: Date format normalization (signup_date, date_of_birth, last_order_date)
    for date_field in ["signup_date", "date_of_birth", "last_order_date"]:
        if data.get(date_field):
            original_date = data[date_field]
            normalized_date = _normalize_date(original_date)
            if normalized_date != original_date:
                data[date_field] = normalized_date
                issues.append(ValidationIssue(
                    run_id=run_id,
                    customer_id=data.get("customer_id"),
                    row_number=data["row_number"],
                    field=date_field,
                    rule_id="R34_DATE_NORMALIZE",
                    severity=Severity.CLEANED,
                    message=f"Date normalized from '{original_date}' to '{normalized_date}'",
                    original_value=original_date,
                    corrected_value=normalized_date,
                ))
            # After normalization, verify the date is valid ISO 8601.
            # If it still cannot be parsed, set optional date fields to NULL.
            current_val = data[date_field]
            if current_val and not DATE_ISO_PATTERN.match(current_val):
                # Non-ISO date that could not be normalized
                if date_field != "signup_date":
                    # Optional date fields get set to NULL
                    data[date_field] = None
                    issues.append(ValidationIssue(
                        run_id=run_id,
                        customer_id=data.get("customer_id"),
                        row_number=data["row_number"],
                        field=date_field,
                        rule_id="R34_DATE_NORMALIZE",
                        severity=Severity.CLEANED,
                        message=f"Unparseable date '{current_val}' set to NULL",
                        original_value=current_val,
                        corrected_value=None,
                    ))
            elif current_val and DATE_ISO_PATTERN.match(current_val):
                # Validate that the ISO date is actually a real date
                parsed = _parse_date(current_val)
                if parsed is None and date_field != "signup_date":
                    data[date_field] = None
                    issues.append(ValidationIssue(
                        run_id=run_id,
                        customer_id=data.get("customer_id"),
                        row_number=data["row_number"],
                        field=date_field,
                        rule_id="R34_DATE_NORMALIZE",
                        severity=Severity.CLEANED,
                        message=f"Invalid date '{current_val}' set to NULL",
                        original_value=current_val,
                        corrected_value=None,
                    ))

    # Rebuild the record with cleaned data
    cleaned_record = RawCustomerRecord(**data)
    return cleaned_record, issues


# ---------------------------------------------------------------------------
# ERROR rules (1-15) -- record rejected
# ---------------------------------------------------------------------------

def run_error_rules(
    record: RawCustomerRecord,
    run_id: str,
    seen_emails: set[str],
    today: date = TODAY,
) -> list[ValidationIssue]:
    """
    Run all ERROR-severity validation rules on a cleaned record.

    Args:
        record: The cleaned RawCustomerRecord.
        run_id: The pipeline run ID.
        seen_emails: Set of already-seen email addresses (lowercase).
                     This function will add the current email if it passes.
        today: Today's date for future-date checks.

    Returns:
        List of ERROR-severity ValidationIssue objects.
    """
    issues: list[ValidationIssue] = []
    data = record.model_dump()

    # Rule 1: Missing email address
    if not data.get("email"):
        issues.append(ValidationIssue(
            run_id=run_id,
            customer_id=data.get("customer_id"),
            row_number=data["row_number"],
            field="email",
            rule_id="R01_MISSING_EMAIL",
            severity=Severity.ERROR,
            message="Missing email address (required field)",
            original_value=None,
        ))
    else:
        email = data["email"]
        email_lower = email.lower()

        # Rule 2: Invalid email format
        email_invalid = False
        if DOUBLE_AT_PATTERN.search(email):
            issues.append(ValidationIssue(
                run_id=run_id,
                customer_id=data.get("customer_id"),
                row_number=data["row_number"],
                field="email",
                rule_id="R02_INVALID_EMAIL",
                severity=Severity.ERROR,
                message=f"Invalid email format: double @@ in '{email}'",
                original_value=email,
            ))
            email_invalid = True
        elif LEADING_DOT_LOCAL.search(email):
            issues.append(ValidationIssue(
                run_id=run_id,
                customer_id=data.get("customer_id"),
                row_number=data["row_number"],
                field="email",
                rule_id="R02_INVALID_EMAIL",
                severity=Severity.ERROR,
                message=f"Invalid email format: leading dot in local part '{email}'",
                original_value=email,
            ))
            email_invalid = True
        elif TRAILING_DOT_LOCAL.search(email):
            issues.append(ValidationIssue(
                run_id=run_id,
                customer_id=data.get("customer_id"),
                row_number=data["row_number"],
                field="email",
                rule_id="R02_INVALID_EMAIL",
                severity=Severity.ERROR,
                message=f"Invalid email format: trailing dot in local part '{email}'",
                original_value=email,
            ))
            email_invalid = True
        elif CONSECUTIVE_DOTS.search(email.split("@")[0] if "@" in email else email):
            issues.append(ValidationIssue(
                run_id=run_id,
                customer_id=data.get("customer_id"),
                row_number=data["row_number"],
                field="email",
                rule_id="R02_INVALID_EMAIL",
                severity=Severity.ERROR,
                message=f"Invalid email format: consecutive dots in '{email}'",
                original_value=email,
            ))
            email_invalid = True
        elif not EMAIL_PATTERN.match(email):
            issues.append(ValidationIssue(
                run_id=run_id,
                customer_id=data.get("customer_id"),
                row_number=data["row_number"],
                field="email",
                rule_id="R02_INVALID_EMAIL",
                severity=Severity.ERROR,
                message=f"Invalid email format: '{email}'",
                original_value=email,
            ))
            email_invalid = True

        # Rule 3: Duplicate email (keep first, reject subsequent)
        if not email_invalid:
            if email_lower in seen_emails:
                issues.append(ValidationIssue(
                    run_id=run_id,
                    customer_id=data.get("customer_id"),
                    row_number=data["row_number"],
                    field="email",
                    rule_id="R03_DUPLICATE_EMAIL",
                    severity=Severity.ERROR,
                    message=f"Duplicate email address: '{email}'",
                    original_value=email,
                ))
            else:
                seen_emails.add(email_lower)

    # Rule 4: Missing both first_name AND last_name
    if not data.get("first_name") and not data.get("last_name"):
        issues.append(ValidationIssue(
            run_id=run_id,
            customer_id=data.get("customer_id"),
            row_number=data["row_number"],
            field="first_name,last_name",
            rule_id="R04_MISSING_BOTH_NAMES",
            severity=Severity.ERROR,
            message="Missing both first name and last name",
        ))

    # Date-based rules (5, 6, 7)
    dob_str = data.get("date_of_birth")
    if dob_str:
        dob = _parse_date(dob_str)
        if dob:
            # Rule 5: Future birth date
            if dob > today:
                issues.append(ValidationIssue(
                    run_id=run_id,
                    customer_id=data.get("customer_id"),
                    row_number=data["row_number"],
                    field="date_of_birth",
                    rule_id="R05_FUTURE_DOB",
                    severity=Severity.ERROR,
                    message=f"Date of birth is in the future: {dob_str}",
                    original_value=dob_str,
                ))
            else:
                # Calculate age
                age = today.year - dob.year
                if (today.month, today.day) < (dob.month, dob.day):
                    age -= 1

                # Rule 6: Under 13 (COPPA)
                if age < MIN_CUSTOMER_AGE:
                    issues.append(ValidationIssue(
                        run_id=run_id,
                        customer_id=data.get("customer_id"),
                        row_number=data["row_number"],
                        field="date_of_birth",
                        rule_id="R06_UNDER_13_COPPA",
                        severity=Severity.ERROR,
                        message=f"Customer is under 13 (age {age}, COPPA violation): {dob_str}",
                        original_value=dob_str,
                    ))

                # Rule 7: Over 120
                if age > MAX_CUSTOMER_AGE:
                    issues.append(ValidationIssue(
                        run_id=run_id,
                        customer_id=data.get("customer_id"),
                        row_number=data["row_number"],
                        field="date_of_birth",
                        rule_id="R07_OVER_120",
                        severity=Severity.ERROR,
                        message=f"Customer is over 120 (age {age}, likely data error): {dob_str}",
                        original_value=dob_str,
                    ))

    # Rule 8: Negative total_spend
    spend = _parse_float(data.get("total_spend"))
    if spend is not None and spend < 0:
        issues.append(ValidationIssue(
            run_id=run_id,
            customer_id=data.get("customer_id"),
            row_number=data["row_number"],
            field="total_spend",
            rule_id="R08_NEGATIVE_SPEND",
            severity=Severity.ERROR,
            message=f"Negative total spend (sentinel value): {data.get('total_spend')}",
            original_value=data.get("total_spend"),
        ))

    # Rule 9: Invalid zip code
    zip_val = data.get("zip_code")
    if zip_val is not None:
        zip_digits = re.sub(r"\D", "", zip_val)
        if not ZIP_CODE_PATTERN.match(zip_digits):
            issues.append(ValidationIssue(
                run_id=run_id,
                customer_id=data.get("customer_id"),
                row_number=data["row_number"],
                field="zip_code",
                rule_id="R09_INVALID_ZIP",
                severity=Severity.ERROR,
                message=f"Invalid zip code (not 5 digits after cleaning): '{zip_val}'",
                original_value=zip_val,
            ))

    # Rule 10: Missing signup_date
    if not data.get("signup_date"):
        issues.append(ValidationIssue(
            run_id=run_id,
            customer_id=data.get("customer_id"),
            row_number=data["row_number"],
            field="signup_date",
            rule_id="R10_MISSING_SIGNUP_DATE",
            severity=Severity.ERROR,
            message="Missing signup date (required for customer timeline)",
        ))

    signup_date = _parse_date(data.get("signup_date"))
    last_order_date = _parse_date(data.get("last_order_date"))

    # Rule 11: Last order date before signup date (skip if last_order_date is NULL)
    if signup_date and last_order_date:
        if last_order_date < signup_date:
            issues.append(ValidationIssue(
                run_id=run_id,
                customer_id=data.get("customer_id"),
                row_number=data["row_number"],
                field="last_order_date",
                rule_id="R11_ORDER_BEFORE_SIGNUP",
                severity=Severity.ERROR,
                message=f"Last order date ({data.get('last_order_date')}) is before signup date ({data.get('signup_date')})",
                original_value=data.get("last_order_date"),
            ))

    # Rule 12: Signup date in the future
    if signup_date and signup_date > today:
        issues.append(ValidationIssue(
            run_id=run_id,
            customer_id=data.get("customer_id"),
            row_number=data["row_number"],
            field="signup_date",
            rule_id="R12_FUTURE_SIGNUP",
            severity=Severity.ERROR,
            message=f"Signup date is in the future: {data.get('signup_date')}",
            original_value=data.get("signup_date"),
        ))

    # Rule 13: Last order date in the future
    if last_order_date and last_order_date > today:
        issues.append(ValidationIssue(
            run_id=run_id,
            customer_id=data.get("customer_id"),
            row_number=data["row_number"],
            field="last_order_date",
            rule_id="R13_FUTURE_LAST_ORDER",
            severity=Severity.ERROR,
            message=f"Last order date is in the future: {data.get('last_order_date')}",
            original_value=data.get("last_order_date"),
        ))

    # Rule 14: Zero orders but positive spend
    # SKIP if both are NULL (valid new customer)
    orders = _parse_int(data.get("num_orders"))
    if spend is not None or orders is not None:
        # Not both NULL
        if orders is not None and orders == 0 and spend is not None and spend > 0:
            issues.append(ValidationIssue(
                run_id=run_id,
                customer_id=data.get("customer_id"),
                row_number=data["row_number"],
                field="num_orders,total_spend",
                rule_id="R14_ZERO_ORDERS_POSITIVE_SPEND",
                severity=Severity.ERROR,
                message=f"Zero orders but positive spend (${spend})",
                original_value=f"num_orders=0, total_spend={spend}",
            ))

    # Rule 15: Positive orders but zero or missing spend
    # NULL spend with positive orders IS an error. SKIP if both NULL.
    if spend is not None or orders is not None:
        # Not both NULL
        if orders is not None and orders > 0:
            if spend is None or spend == 0:
                issues.append(ValidationIssue(
                    run_id=run_id,
                    customer_id=data.get("customer_id"),
                    row_number=data["row_number"],
                    field="num_orders,total_spend",
                    rule_id="R15_POSITIVE_ORDERS_NO_SPEND",
                    severity=Severity.ERROR,
                    message=f"Positive orders ({orders}) but zero or missing spend",
                    original_value=f"num_orders={orders}, total_spend={data.get('total_spend')}",
                ))

    return issues


# ---------------------------------------------------------------------------
# WARNING rules (16-28) -- record loads but flagged
# ---------------------------------------------------------------------------

def run_warning_rules(
    record: RawCustomerRecord,
    run_id: str,
    fuzzy_dupes: dict[str, list[tuple[str, int]]],
) -> list[ValidationIssue]:
    """
    Run all WARNING-severity validation rules on a cleaned record.

    Args:
        record: The cleaned RawCustomerRecord.
        run_id: The pipeline run ID.
        fuzzy_dupes: Dict tracking normalized (last_name_lower + "|" + zip)
                     to list of (email, row_number) for fuzzy dupe detection.

    Returns:
        List of WARNING-severity ValidationIssue objects.
    """
    issues: list[ValidationIssue] = []
    data = record.model_dump()

    # Rule 16: Missing first_name (but last_name exists)
    if not data.get("first_name") and data.get("last_name"):
        issues.append(ValidationIssue(
            run_id=run_id,
            customer_id=data.get("customer_id"),
            row_number=data["row_number"],
            field="first_name",
            rule_id="R16_MISSING_FIRST_NAME",
            severity=Severity.WARNING,
            message="Missing first name",
        ))

    # Rule 17: Missing last_name (but first_name exists)
    if not data.get("last_name") and data.get("first_name"):
        issues.append(ValidationIssue(
            run_id=run_id,
            customer_id=data.get("customer_id"),
            row_number=data["row_number"],
            field="last_name",
            rule_id="R17_MISSING_LAST_NAME",
            severity=Severity.WARNING,
            message="Missing last name",
        ))

    # Rule 18: Missing phone
    if not data.get("phone"):
        issues.append(ValidationIssue(
            run_id=run_id,
            customer_id=data.get("customer_id"),
            row_number=data["row_number"],
            field="phone",
            rule_id="R18_MISSING_PHONE",
            severity=Severity.WARNING,
            message="Missing phone number",
        ))

    # Rule 19: Missing DOB
    if not data.get("date_of_birth"):
        issues.append(ValidationIssue(
            run_id=run_id,
            customer_id=data.get("customer_id"),
            row_number=data["row_number"],
            field="date_of_birth",
            rule_id="R19_MISSING_DOB",
            severity=Severity.WARNING,
            message="Missing date of birth",
        ))

    # Rule 20: Missing loyalty_tier
    if not data.get("loyalty_tier"):
        issues.append(ValidationIssue(
            run_id=run_id,
            customer_id=data.get("customer_id"),
            row_number=data["row_number"],
            field="loyalty_tier",
            rule_id="R20_MISSING_LOYALTY_TIER",
            severity=Severity.WARNING,
            message="Missing loyalty tier",
        ))

    # Rule 21: Missing preferred_contact
    if not data.get("preferred_contact"):
        issues.append(ValidationIssue(
            run_id=run_id,
            customer_id=data.get("customer_id"),
            row_number=data["row_number"],
            field="preferred_contact",
            rule_id="R21_MISSING_PREFERRED_CONTACT",
            severity=Severity.WARNING,
            message="Missing preferred contact method",
        ))

    # Rule 22: Name contains digits or special characters
    for name_field in ["first_name", "last_name"]:
        name_val = data.get(name_field)
        if name_val and NAME_SUSPICIOUS_CHARS.search(name_val):
            issues.append(ValidationIssue(
                run_id=run_id,
                customer_id=data.get("customer_id"),
                row_number=data["row_number"],
                field=name_field,
                rule_id="R22_NAME_SUSPICIOUS_CHARS",
                severity=Severity.WARNING,
                message=f"{name_field} contains digits or special characters: '{name_val}'",
                original_value=name_val,
            ))

    # Rule 23: Single-character name
    for name_field in ["first_name", "last_name"]:
        name_val = data.get(name_field)
        if name_val and len(name_val.strip()) == 1:
            issues.append(ValidationIssue(
                run_id=run_id,
                customer_id=data.get("customer_id"),
                row_number=data["row_number"],
                field=name_field,
                rule_id="R23_SINGLE_CHAR_NAME",
                severity=Severity.WARNING,
                message=f"{name_field} is a single character: '{name_val}'",
                original_value=name_val,
            ))

    # Rule 24: City contains digits
    city_val = data.get("city")
    if city_val and CITY_HAS_DIGITS.search(city_val):
        issues.append(ValidationIssue(
            run_id=run_id,
            customer_id=data.get("customer_id"),
            row_number=data["row_number"],
            field="city",
            rule_id="R24_CITY_HAS_DIGITS",
            severity=Severity.WARNING,
            message=f"City contains digits: '{city_val}'",
            original_value=city_val,
        ))

    # Rule 25: Tier-spend mismatch
    tier_val = data.get("loyalty_tier")
    spend = _parse_float(data.get("total_spend"))
    if tier_val and spend is not None:
        thresholds = TIER_SPEND_THRESHOLDS.get(tier_val)
        if thresholds:
            if "max_below" in thresholds and spend < thresholds["max_below"]:
                issues.append(ValidationIssue(
                    run_id=run_id,
                    customer_id=data.get("customer_id"),
                    row_number=data["row_number"],
                    field="loyalty_tier,total_spend",
                    rule_id="R25_TIER_SPEND_MISMATCH",
                    severity=Severity.WARNING,
                    message=f"{tier_val} tier with only ${spend:.2f} total spend",
                    original_value=f"tier={tier_val}, spend={spend}",
                ))
            if "min_above" in thresholds and spend > thresholds["min_above"]:
                issues.append(ValidationIssue(
                    run_id=run_id,
                    customer_id=data.get("customer_id"),
                    row_number=data["row_number"],
                    field="loyalty_tier,total_spend",
                    rule_id="R25_TIER_SPEND_MISMATCH",
                    severity=Severity.WARNING,
                    message=f"{tier_val} tier with ${spend:.2f} total spend (high for tier)",
                    original_value=f"tier={tier_val}, spend={spend}",
                ))

    # Rule 26: AOV outlier
    orders = _parse_int(data.get("num_orders"))
    if spend is not None and orders is not None and orders > 0:
        aov = spend / orders
        if aov > AOV_OUTLIER_MAX:
            issues.append(ValidationIssue(
                run_id=run_id,
                customer_id=data.get("customer_id"),
                row_number=data["row_number"],
                field="total_spend,num_orders",
                rule_id="R26_AOV_OUTLIER",
                severity=Severity.WARNING,
                message=f"Average order value outlier: ${aov:.2f} (spend={spend}, orders={orders})",
                original_value=f"aov={aov:.2f}",
            ))
        elif aov < AOV_OUTLIER_MIN:
            issues.append(ValidationIssue(
                run_id=run_id,
                customer_id=data.get("customer_id"),
                row_number=data["row_number"],
                field="total_spend,num_orders",
                rule_id="R26_AOV_OUTLIER",
                severity=Severity.WARNING,
                message=f"Average order value outlier: ${aov:.2f} (spend={spend}, orders={orders})",
                original_value=f"aov={aov:.2f}",
            ))

    # Rule 27: Phone missing area code (7 digits only)
    phone_val = data.get("phone")
    if phone_val:
        phone_digits = re.sub(r"\D", "", phone_val)
        if PHONE_7_DIGITS_PATTERN.match(phone_digits):
            issues.append(ValidationIssue(
                run_id=run_id,
                customer_id=data.get("customer_id"),
                row_number=data["row_number"],
                field="phone",
                rule_id="R27_PHONE_MISSING_AREA_CODE",
                severity=Severity.WARNING,
                message=f"Phone number appears to be missing area code (7 digits): '{phone_val}'",
                original_value=phone_val,
            ))

    # Rule 28: Fuzzy duplicate (same normalized last_name + same zip + different email)
    # SKIP if last_name or zip is missing
    last_name = data.get("last_name")
    zip_code = data.get("zip_code")
    email = data.get("email")
    if last_name and zip_code and email:
        fuzzy_key = f"{last_name.lower().strip()}|{zip_code.strip()}"
        if fuzzy_key in fuzzy_dupes:
            # Check if there is an existing entry with a different email
            existing_entries = fuzzy_dupes[fuzzy_key]
            for existing_email, existing_row in existing_entries:
                if existing_email.lower() != email.lower():
                    issues.append(ValidationIssue(
                        run_id=run_id,
                        customer_id=data.get("customer_id"),
                        row_number=data["row_number"],
                        field="last_name,zip_code,email",
                        rule_id="R28_FUZZY_DUPLICATE",
                        severity=Severity.WARNING,
                        message=(
                            f"Potential duplicate: same last name '{last_name}' and "
                            f"zip '{zip_code}' as row {existing_row} but different email"
                        ),
                        original_value=email,
                    ))
                    break  # Only warn once per match
            fuzzy_dupes[fuzzy_key].append((email, data["row_number"]))
        else:
            fuzzy_dupes[fuzzy_key] = [(email, data["row_number"])]

    return issues


# ---------------------------------------------------------------------------
# Main validation entry point
# ---------------------------------------------------------------------------

def validate_record(
    record: RawCustomerRecord,
    run_id: str,
    row_meta: dict,
    seen_emails: set[str],
    fuzzy_dupes: dict[str, list[tuple[str, int]]],
) -> tuple[RawCustomerRecord, list[ValidationIssue], bool]:
    """
    Run all validation rules on a single record.

    Execution order:
    1. CLEANED rules (normalize data)
    2. ERROR rules (reject bad records)
    3. WARNING rules (flag concerns on valid records)

    Args:
        record: The raw customer record.
        run_id: The pipeline run ID.
        row_meta: Metadata from the loader about whitespace/null sentinels.
        seen_emails: Shared set of seen email addresses for dupe detection.
        fuzzy_dupes: Shared dict for fuzzy duplicate detection.

    Returns:
        Tuple of (cleaned_record, all_issues, has_errors).
    """
    # Step 1: Run cleaning rules (normalizes the record in-place)
    cleaned_record, clean_issues = run_cleaning_rules(record, run_id, row_meta)

    # Step 2: Run error rules on cleaned data
    error_issues = run_error_rules(cleaned_record, run_id, seen_emails)

    # Step 3: Run warning rules on cleaned data (only if no errors,
    # but we still run them for reporting purposes)
    warning_issues = run_warning_rules(cleaned_record, run_id, fuzzy_dupes)

    all_issues = clean_issues + error_issues + warning_issues
    has_errors = len(error_issues) > 0

    return cleaned_record, all_issues, has_errors
