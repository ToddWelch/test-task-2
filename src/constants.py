"""
Constants for the customer data pipeline.

Contains reference data for validation and normalization:
state mappings, loyalty tiers, contact methods, thresholds,
regex patterns, and disposable email domains.
"""

import re

# ---------------------------------------------------------------------------
# US State name-to-abbreviation mapping
# All 50 states + DC + territories
# Keys are lowercase for case-insensitive lookup
# ---------------------------------------------------------------------------

STATE_ABBREVIATIONS: dict[str, str] = {
    # 50 States
    "alabama": "AL",
    "alaska": "AK",
    "arizona": "AZ",
    "arkansas": "AR",
    "california": "CA",
    "colorado": "CO",
    "connecticut": "CT",
    "delaware": "DE",
    "florida": "FL",
    "georgia": "GA",
    "hawaii": "HI",
    "idaho": "ID",
    "illinois": "IL",
    "indiana": "IN",
    "iowa": "IA",
    "kansas": "KS",
    "kentucky": "KY",
    "louisiana": "LA",
    "maine": "ME",
    "maryland": "MD",
    "massachusetts": "MA",
    "michigan": "MI",
    "minnesota": "MN",
    "mississippi": "MS",
    "missouri": "MO",
    "montana": "MT",
    "nebraska": "NE",
    "nevada": "NV",
    "new hampshire": "NH",
    "new jersey": "NJ",
    "new mexico": "NM",
    "new york": "NY",
    "north carolina": "NC",
    "north dakota": "ND",
    "ohio": "OH",
    "oklahoma": "OK",
    "oregon": "OR",
    "pennsylvania": "PA",
    "rhode island": "RI",
    "south carolina": "SC",
    "south dakota": "SD",
    "tennessee": "TN",
    "texas": "TX",
    "utah": "UT",
    "vermont": "VT",
    "virginia": "VA",
    "washington": "WA",
    "west virginia": "WV",
    "wisconsin": "WI",
    "wyoming": "WY",
    # District of Columbia
    "district of columbia": "DC",
    # Territories
    "american samoa": "AS",
    "guam": "GU",
    "northern mariana islands": "MP",
    "puerto rico": "PR",
    "us virgin islands": "VI",
    "u.s. virgin islands": "VI",
}

# Set of valid two-letter state/territory abbreviations for quick lookup
VALID_STATE_ABBREVS: set[str] = set(STATE_ABBREVIATIONS.values())

# ---------------------------------------------------------------------------
# Loyalty tiers (canonical casing)
# ---------------------------------------------------------------------------

VALID_LOYALTY_TIERS: list[str] = ["Bronze", "Silver", "Gold", "Platinum"]

# Lowercase lookup for case-insensitive matching
LOYALTY_TIER_LOOKUP: dict[str, str] = {
    tier.lower(): tier for tier in VALID_LOYALTY_TIERS
}

# ---------------------------------------------------------------------------
# Preferred contact methods
# ---------------------------------------------------------------------------

VALID_PREFERRED_CONTACT: list[str] = ["email", "phone", "mail"]

# ---------------------------------------------------------------------------
# Tier-spend thresholds (Rule 25)
#
# These flag suspicious combinations where the loyalty tier does not
# match the spending level. Not necessarily errors, but worth flagging.
# ---------------------------------------------------------------------------

TIER_SPEND_THRESHOLDS: dict[str, dict[str, float]] = {
    # Platinum customers with less than $100 total spend are suspicious
    "Platinum": {"max_below": 100.0},
    # Bronze customers with more than $4000 total spend are suspicious
    "Bronze": {"min_above": 4000.0},
}

# ---------------------------------------------------------------------------
# Average order value outlier thresholds (Rule 26)
#
# AOV = total_spend / num_orders
# Values outside this range are flagged as warnings.
# ---------------------------------------------------------------------------

AOV_OUTLIER_MIN: float = 1.0
AOV_OUTLIER_MAX: float = 1000.0

# ---------------------------------------------------------------------------
# Disposable email domains
#
# Records with emails from these domains are not rejected, but they
# could be used as a warning signal in future rules. Included here
# for completeness and potential Rule expansion.
# ---------------------------------------------------------------------------

DISPOSABLE_EMAIL_DOMAINS: set[str] = {
    "mailinator.com",
    "guerrillamail.com",
    "guerrillamail.de",
    "guerrillamail.net",
    "guerrillamail.org",
    "grr.la",
    "tempmail.com",
    "temp-mail.org",
    "throwaway.email",
    "yopmail.com",
    "sharklasers.com",
    "guerrillamailblock.com",
    "10minutemail.com",
    "trashmail.com",
    "trashmail.net",
    "dispostable.com",
    "maildrop.cc",
    "mailnesia.com",
    "mailcatch.com",
    "mintemail.com",
    "getairmail.com",
    "getnada.com",
    "mohmal.com",
    "fakeinbox.com",
}

# ---------------------------------------------------------------------------
# Regex patterns
# ---------------------------------------------------------------------------

# Email validation: basic structural check.
# Covers: local part, @, domain with TLD.
# IMPORTANT: This pattern alone is NOT a complete email validator. It accepts
# consecutive dots and trailing dots in the local part. The validator must
# also check DOUBLE_AT_PATTERN, LEADING_DOT_LOCAL, TRAILING_DOT_LOCAL, and
# CONSECUTIVE_DOTS before falling through to this pattern.
EMAIL_PATTERN: re.Pattern[str] = re.compile(
    r"^[a-zA-Z0-9]"                    # Must start with alphanumeric
    r"[a-zA-Z0-9.!#$%&'*+/=?^_`{|}~-]*"  # Local part body
    r"@"                                # Single @
    r"[a-zA-Z0-9]"                      # Domain starts with alphanumeric
    r"(?:[a-zA-Z0-9-]*[a-zA-Z0-9])?"   # Optional domain label body
    r"(?:\.[a-zA-Z0-9](?:[a-zA-Z0-9-]*[a-zA-Z0-9])?)*"  # Additional labels
    r"\.[a-zA-Z]{2,}$"                  # TLD (2+ alpha chars)
)

# Quick check for double @@ (an explicit rejection rule)
DOUBLE_AT_PATTERN: re.Pattern[str] = re.compile(r"@@")

# Leading dot in email local part
LEADING_DOT_LOCAL: re.Pattern[str] = re.compile(r"^\..*@")

# Trailing dot in email local part
TRAILING_DOT_LOCAL: re.Pattern[str] = re.compile(r"\.@")

# Consecutive dots in email local part
CONSECUTIVE_DOTS: re.Pattern[str] = re.compile(r"\.\.")

# Phone: match 10 digits (with optional country code 1 prefix).
# Used after stripping all non-digit characters.
PHONE_DIGITS_PATTERN: re.Pattern[str] = re.compile(r"^1?(\d{10})$")

# Phone: match exactly 7 digits (missing area code)
PHONE_7_DIGITS_PATTERN: re.Pattern[str] = re.compile(r"^(\d{7})$")

# Zip code: exactly 5 digits
ZIP_CODE_PATTERN: re.Pattern[str] = re.compile(r"^\d{5}$")

# Name validation: flag names containing digits or special characters
# (allows letters, spaces, hyphens, apostrophes, and periods)
NAME_SUSPICIOUS_CHARS: re.Pattern[str] = re.compile(r"[^a-zA-Z\s\-\'\.]+")

# City validation: flag cities containing digits
CITY_HAS_DIGITS: re.Pattern[str] = re.compile(r"\d")

# Date patterns for normalization (MM/DD/YYYY, MM-DD-YYYY, M/DD/YY, etc.)
DATE_SLASH_PATTERN: re.Pattern[str] = re.compile(
    r"^(\d{1,2})/(\d{1,2})/(\d{2,4})$"
)
DATE_DASH_MDY_PATTERN: re.Pattern[str] = re.compile(
    r"^(\d{1,2})-(\d{1,2})-(\d{2,4})$"
)
DATE_ISO_PATTERN: re.Pattern[str] = re.compile(
    r"^\d{4}-\d{2}-\d{2}$"
)

# ---------------------------------------------------------------------------
# Newsletter boolean normalization (Rule 33)
#
# Truthy values map to True, falsy values map to False.
# Empty/NULL stays NULL.
# ---------------------------------------------------------------------------

NEWSLETTER_TRUTHY: set[str] = {
    "true", "1", "yes", "y", "t",
}

NEWSLETTER_FALSY: set[str] = {
    "false", "0", "no", "n", "f",
}

# ---------------------------------------------------------------------------
# Values treated as NULL/missing across all fields (Rule 35)
# ---------------------------------------------------------------------------

NULL_SENTINEL_VALUES: set[str] = {
    "n/a", "na", "none", "null", "",
}

# ---------------------------------------------------------------------------
# Age boundaries for DOB validation
# ---------------------------------------------------------------------------

MIN_CUSTOMER_AGE: int = 13    # COPPA compliance (Rule 6)
MAX_CUSTOMER_AGE: int = 120   # Data error threshold (Rule 7)
