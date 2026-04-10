"""
Unit tests for all 35 validation rules in src/validators.py.

Tests cover known-good inputs, known-bad inputs, and edge cases
including empty strings, None values, boundary dates, and Unicode.
Uses TODAY = date(2026, 4, 10) matching the hardcoded date in validators.py.
"""

import unittest
from datetime import date

from src.models import RawCustomerRecord, Severity
from src.validators import (
    run_cleaning_rules,
    run_error_rules,
    run_warning_rules,
    validate_record,
    _normalize_date,
    _normalize_phone,
    _normalize_state,
    _normalize_loyalty_tier,
    _normalize_newsletter,
    TODAY,
)


class TestHelperFunctions(unittest.TestCase):
    """Tests for internal helper/normalization functions."""

    # ---------------------------------------------------------------
    # _normalize_date
    # ---------------------------------------------------------------

    def test_normalize_date_iso_passthrough(self):
        self.assertEqual(_normalize_date("2020-01-15"), "2020-01-15")

    def test_normalize_date_slash_format(self):
        self.assertEqual(_normalize_date("01/15/2020"), "2020-01-15")

    def test_normalize_date_dash_mdy_format(self):
        self.assertEqual(_normalize_date("01-15-2020"), "2020-01-15")

    def test_normalize_date_two_digit_year_low(self):
        # Two-digit year < 30 -> 2000s
        self.assertEqual(_normalize_date("3/15/25"), "2025-03-15")

    def test_normalize_date_two_digit_year_high(self):
        # Two-digit year >= 30 -> 1900s
        self.assertEqual(_normalize_date("3/15/85"), "1985-03-15")

    def test_normalize_date_none(self):
        self.assertIsNone(_normalize_date(None))

    def test_normalize_date_empty_string(self):
        # Empty string is falsy, returns as-is
        self.assertEqual(_normalize_date(""), "")

    def test_normalize_date_invalid_returns_original(self):
        self.assertEqual(_normalize_date("not-a-date"), "not-a-date")

    def test_normalize_date_invalid_month_returns_original(self):
        # 13/01/2020 has invalid month
        self.assertEqual(_normalize_date("13/01/2020"), "13/01/2020")

    # ---------------------------------------------------------------
    # _normalize_phone
    # ---------------------------------------------------------------

    def test_normalize_phone_ten_digits(self):
        self.assertEqual(_normalize_phone("5551234567"), "(555) 123-4567")

    def test_normalize_phone_with_country_code(self):
        self.assertEqual(_normalize_phone("15551234567"), "(555) 123-4567")

    def test_normalize_phone_dashes(self):
        self.assertEqual(_normalize_phone("555-123-4567"), "(555) 123-4567")

    def test_normalize_phone_parens(self):
        self.assertEqual(_normalize_phone("(555) 123-4567"), "(555) 123-4567")

    def test_normalize_phone_dots(self):
        self.assertEqual(_normalize_phone("555.123.4567"), "(555) 123-4567")

    def test_normalize_phone_seven_digits_unchanged(self):
        # 7 digits missing area code; left as-is
        self.assertEqual(_normalize_phone("1234567"), "1234567")

    def test_normalize_phone_none(self):
        self.assertIsNone(_normalize_phone(None))

    def test_normalize_phone_empty(self):
        self.assertEqual(_normalize_phone(""), "")

    # ---------------------------------------------------------------
    # _normalize_state
    # ---------------------------------------------------------------

    def test_normalize_state_full_name(self):
        self.assertEqual(_normalize_state("california"), "CA")

    def test_normalize_state_full_name_mixed_case(self):
        self.assertEqual(_normalize_state("Florida"), "FL")

    def test_normalize_state_abbrev_with_periods(self):
        self.assertEqual(_normalize_state("N.Y."), "NY")

    def test_normalize_state_abbrev_lowercase_with_period(self):
        self.assertEqual(_normalize_state("ca."), "CA")

    def test_normalize_state_already_valid(self):
        self.assertEqual(_normalize_state("OH"), "OH")

    def test_normalize_state_lowercase_abbrev(self):
        self.assertEqual(_normalize_state("il"), "IL")

    def test_normalize_state_none(self):
        self.assertIsNone(_normalize_state(None))

    def test_normalize_state_empty(self):
        self.assertEqual(_normalize_state(""), "")

    def test_normalize_state_ohio(self):
        self.assertEqual(_normalize_state("ohio"), "OH")

    # ---------------------------------------------------------------
    # _normalize_loyalty_tier
    # ---------------------------------------------------------------

    def test_normalize_tier_lowercase(self):
        self.assertEqual(_normalize_loyalty_tier("gold"), "Gold")

    def test_normalize_tier_uppercase(self):
        self.assertEqual(_normalize_loyalty_tier("SILVER"), "Silver")

    def test_normalize_tier_mixed_case(self):
        self.assertEqual(_normalize_loyalty_tier("pLaTiNuM"), "Platinum")

    def test_normalize_tier_already_correct(self):
        self.assertEqual(_normalize_loyalty_tier("Bronze"), "Bronze")

    def test_normalize_tier_none(self):
        self.assertIsNone(_normalize_loyalty_tier(None))

    def test_normalize_tier_unknown_value(self):
        self.assertEqual(_normalize_loyalty_tier("Diamond"), "Diamond")

    # ---------------------------------------------------------------
    # _normalize_newsletter
    # ---------------------------------------------------------------

    def test_normalize_newsletter_yes(self):
        self.assertEqual(_normalize_newsletter("yes"), "True")

    def test_normalize_newsletter_y(self):
        self.assertEqual(_normalize_newsletter("Y"), "True")

    def test_normalize_newsletter_one(self):
        self.assertEqual(_normalize_newsletter("1"), "True")

    def test_normalize_newsletter_true(self):
        self.assertEqual(_normalize_newsletter("TRUE"), "True")

    def test_normalize_newsletter_no(self):
        self.assertEqual(_normalize_newsletter("no"), "False")

    def test_normalize_newsletter_zero(self):
        self.assertEqual(_normalize_newsletter("0"), "False")

    def test_normalize_newsletter_false(self):
        self.assertEqual(_normalize_newsletter("False"), "False")

    def test_normalize_newsletter_none(self):
        self.assertIsNone(_normalize_newsletter(None))

    def test_normalize_newsletter_empty(self):
        self.assertIsNone(_normalize_newsletter(""))

    def test_normalize_newsletter_unknown(self):
        self.assertEqual(_normalize_newsletter("maybe"), "maybe")


# ===================================================================
# CLEANED rules (29-35)
# ===================================================================

class TestCleaningRules(unittest.TestCase):
    """Tests for CLEANED severity rules (29-35)."""

    def _make_record(self, **kwargs):
        defaults = {
            "row_number": 1,
            "customer_id": "CUST-TEST",
            "email": "test@example.com",
            "signup_date": "2024-01-01",
        }
        defaults.update(kwargs)
        return RawCustomerRecord(**defaults)

    # Rule 29: Whitespace trimming (logged from loader metadata)
    def test_r29_whitespace_logged(self):
        record = self._make_record(first_name="John")
        meta = {"whitespace_fields": ["first_name"], "null_sentinel_fields": []}
        _, issues = run_cleaning_rules(record, "run1", meta)
        r29 = [i for i in issues if i.rule_id == "R29_WHITESPACE_TRIM"]
        self.assertEqual(len(r29), 1)
        self.assertEqual(r29[0].field, "first_name")
        self.assertEqual(r29[0].severity, Severity.CLEANED)

    def test_r29_no_whitespace(self):
        record = self._make_record(first_name="John")
        meta = {"whitespace_fields": [], "null_sentinel_fields": []}
        _, issues = run_cleaning_rules(record, "run1", meta)
        r29 = [i for i in issues if i.rule_id == "R29_WHITESPACE_TRIM"]
        self.assertEqual(len(r29), 0)

    # Rule 30: State normalization
    def test_r30_state_normalized(self):
        record = self._make_record(state="california")
        meta = {"whitespace_fields": [], "null_sentinel_fields": []}
        cleaned, issues = run_cleaning_rules(record, "run1", meta)
        r30 = [i for i in issues if i.rule_id == "R30_STATE_NORMALIZE"]
        self.assertEqual(len(r30), 1)
        self.assertEqual(r30[0].corrected_value, "CA")
        self.assertEqual(cleaned.state, "CA")

    def test_r30_state_already_valid(self):
        record = self._make_record(state="CA")
        meta = {"whitespace_fields": [], "null_sentinel_fields": []}
        _, issues = run_cleaning_rules(record, "run1", meta)
        r30 = [i for i in issues if i.rule_id == "R30_STATE_NORMALIZE"]
        self.assertEqual(len(r30), 0)

    def test_r30_state_with_periods(self):
        record = self._make_record(state="N.Y.")
        meta = {"whitespace_fields": [], "null_sentinel_fields": []}
        cleaned, issues = run_cleaning_rules(record, "run1", meta)
        r30 = [i for i in issues if i.rule_id == "R30_STATE_NORMALIZE"]
        self.assertEqual(len(r30), 1)
        self.assertEqual(cleaned.state, "NY")

    # Rule 31: Phone normalization
    def test_r31_phone_normalized(self):
        record = self._make_record(phone="555-123-4567")
        meta = {"whitespace_fields": [], "null_sentinel_fields": []}
        cleaned, issues = run_cleaning_rules(record, "run1", meta)
        r31 = [i for i in issues if i.rule_id == "R31_PHONE_NORMALIZE"]
        self.assertEqual(len(r31), 1)
        self.assertEqual(cleaned.phone, "(555) 123-4567")

    def test_r31_phone_already_formatted(self):
        record = self._make_record(phone="(555) 123-4567")
        meta = {"whitespace_fields": [], "null_sentinel_fields": []}
        _, issues = run_cleaning_rules(record, "run1", meta)
        r31 = [i for i in issues if i.rule_id == "R31_PHONE_NORMALIZE"]
        self.assertEqual(len(r31), 0)

    # Rule 32: Loyalty tier normalization
    def test_r32_tier_normalized(self):
        record = self._make_record(loyalty_tier="SILVER")
        meta = {"whitespace_fields": [], "null_sentinel_fields": []}
        cleaned, issues = run_cleaning_rules(record, "run1", meta)
        r32 = [i for i in issues if i.rule_id == "R32_TIER_NORMALIZE"]
        self.assertEqual(len(r32), 1)
        self.assertEqual(cleaned.loyalty_tier, "Silver")

    def test_r32_tier_already_correct(self):
        record = self._make_record(loyalty_tier="Gold")
        meta = {"whitespace_fields": [], "null_sentinel_fields": []}
        _, issues = run_cleaning_rules(record, "run1", meta)
        r32 = [i for i in issues if i.rule_id == "R32_TIER_NORMALIZE"]
        self.assertEqual(len(r32), 0)

    # Rule 33: Newsletter boolean normalization
    def test_r33_newsletter_yes_to_true(self):
        record = self._make_record(newsletter_opt_in="yes")
        meta = {"whitespace_fields": [], "null_sentinel_fields": []}
        cleaned, issues = run_cleaning_rules(record, "run1", meta)
        r33 = [i for i in issues if i.rule_id == "R33_NEWSLETTER_NORMALIZE"]
        self.assertEqual(len(r33), 1)
        self.assertEqual(cleaned.newsletter_opt_in, "True")

    def test_r33_newsletter_1_to_true(self):
        record = self._make_record(newsletter_opt_in="1")
        meta = {"whitespace_fields": [], "null_sentinel_fields": []}
        cleaned, issues = run_cleaning_rules(record, "run1", meta)
        r33 = [i for i in issues if i.rule_id == "R33_NEWSLETTER_NORMALIZE"]
        self.assertEqual(len(r33), 1)
        self.assertEqual(cleaned.newsletter_opt_in, "True")

    def test_r33_newsletter_already_true(self):
        record = self._make_record(newsletter_opt_in="True")
        meta = {"whitespace_fields": [], "null_sentinel_fields": []}
        _, issues = run_cleaning_rules(record, "run1", meta)
        r33 = [i for i in issues if i.rule_id == "R33_NEWSLETTER_NORMALIZE"]
        self.assertEqual(len(r33), 0)

    def test_r33_newsletter_none_skipped(self):
        record = self._make_record(newsletter_opt_in=None)
        meta = {"whitespace_fields": [], "null_sentinel_fields": []}
        _, issues = run_cleaning_rules(record, "run1", meta)
        r33 = [i for i in issues if i.rule_id == "R33_NEWSLETTER_NORMALIZE"]
        self.assertEqual(len(r33), 0)

    # Rule 34: Date normalization
    def test_r34_date_slash_normalized(self):
        record = self._make_record(signup_date="01/15/2020")
        meta = {"whitespace_fields": [], "null_sentinel_fields": []}
        cleaned, issues = run_cleaning_rules(record, "run1", meta)
        r34 = [i for i in issues if i.rule_id == "R34_DATE_NORMALIZE"]
        self.assertEqual(len(r34), 1)
        self.assertEqual(cleaned.signup_date, "2020-01-15")

    def test_r34_date_dash_mdy_normalized(self):
        record = self._make_record(date_of_birth="12-25-1990")
        meta = {"whitespace_fields": [], "null_sentinel_fields": []}
        cleaned, issues = run_cleaning_rules(record, "run1", meta)
        r34 = [i for i in issues if i.rule_id == "R34_DATE_NORMALIZE"]
        self.assertEqual(len(r34), 1)
        self.assertEqual(cleaned.date_of_birth, "1990-12-25")

    def test_r34_date_iso_no_change(self):
        record = self._make_record(signup_date="2020-01-15")
        meta = {"whitespace_fields": [], "null_sentinel_fields": []}
        _, issues = run_cleaning_rules(record, "run1", meta)
        r34 = [i for i in issues if i.rule_id == "R34_DATE_NORMALIZE"]
        self.assertEqual(len(r34), 0)

    def test_r34_multiple_date_fields(self):
        record = self._make_record(
            signup_date="01/15/2020",
            date_of_birth="12-25-1990",
            last_order_date="3/5/24",
        )
        meta = {"whitespace_fields": [], "null_sentinel_fields": []}
        cleaned, issues = run_cleaning_rules(record, "run1", meta)
        r34 = [i for i in issues if i.rule_id == "R34_DATE_NORMALIZE"]
        self.assertEqual(len(r34), 3)
        self.assertEqual(cleaned.signup_date, "2020-01-15")
        self.assertEqual(cleaned.date_of_birth, "1990-12-25")
        self.assertEqual(cleaned.last_order_date, "2024-03-05")

    # Rule 35: NULL sentinel conversion (logged from loader metadata)
    def test_r35_null_sentinel_logged(self):
        record = self._make_record(date_of_birth=None)
        meta = {"whitespace_fields": [], "null_sentinel_fields": [("date_of_birth", "N/A")]}
        _, issues = run_cleaning_rules(record, "run1", meta)
        r35 = [i for i in issues if i.rule_id == "R35_NULL_SENTINEL"]
        self.assertEqual(len(r35), 1)
        self.assertEqual(r35[0].original_value, "N/A")

    def test_r35_no_sentinels(self):
        record = self._make_record()
        meta = {"whitespace_fields": [], "null_sentinel_fields": []}
        _, issues = run_cleaning_rules(record, "run1", meta)
        r35 = [i for i in issues if i.rule_id == "R35_NULL_SENTINEL"]
        self.assertEqual(len(r35), 0)


# ===================================================================
# ERROR rules (1-15)
# ===================================================================

class TestErrorRules(unittest.TestCase):
    """Tests for ERROR severity rules (1-15)."""

    def _make_record(self, **kwargs):
        defaults = {
            "row_number": 1,
            "customer_id": "CUST-TEST",
            "first_name": "John",
            "last_name": "Doe",
            "email": "john@example.com",
            "signup_date": "2024-01-01",
            "total_spend": "100.00",
            "num_orders": "5",
        }
        defaults.update(kwargs)
        return RawCustomerRecord(**defaults)

    # Rule 1: Missing email
    def test_r01_missing_email(self):
        record = self._make_record(email=None)
        issues = run_error_rules(record, "run1", set())
        r01 = [i for i in issues if i.rule_id == "R01_MISSING_EMAIL"]
        self.assertEqual(len(r01), 1)

    def test_r01_empty_email(self):
        record = self._make_record(email="")
        issues = run_error_rules(record, "run1", set())
        r01 = [i for i in issues if i.rule_id == "R01_MISSING_EMAIL"]
        self.assertEqual(len(r01), 1)

    def test_r01_valid_email_no_error(self):
        record = self._make_record(email="test@example.com")
        issues = run_error_rules(record, "run1", set())
        r01 = [i for i in issues if i.rule_id == "R01_MISSING_EMAIL"]
        self.assertEqual(len(r01), 0)

    # Rule 2: Invalid email format
    def test_r02_double_at(self):
        record = self._make_record(email="user@@example.com")
        issues = run_error_rules(record, "run1", set())
        r02 = [i for i in issues if i.rule_id == "R02_INVALID_EMAIL"]
        self.assertEqual(len(r02), 1)
        self.assertIn("double @@", r02[0].message)

    def test_r02_leading_dot(self):
        record = self._make_record(email=".user@example.com")
        issues = run_error_rules(record, "run1", set())
        r02 = [i for i in issues if i.rule_id == "R02_INVALID_EMAIL"]
        self.assertEqual(len(r02), 1)
        self.assertIn("leading dot", r02[0].message)

    def test_r02_trailing_dot(self):
        record = self._make_record(email="user.@example.com")
        issues = run_error_rules(record, "run1", set())
        r02 = [i for i in issues if i.rule_id == "R02_INVALID_EMAIL"]
        self.assertEqual(len(r02), 1)
        self.assertIn("trailing dot", r02[0].message)

    def test_r02_consecutive_dots(self):
        record = self._make_record(email="user..name@example.com")
        issues = run_error_rules(record, "run1", set())
        r02 = [i for i in issues if i.rule_id == "R02_INVALID_EMAIL"]
        self.assertEqual(len(r02), 1)
        self.assertIn("consecutive dots", r02[0].message)

    def test_r02_missing_tld(self):
        record = self._make_record(email="user@example")
        issues = run_error_rules(record, "run1", set())
        r02 = [i for i in issues if i.rule_id == "R02_INVALID_EMAIL"]
        self.assertEqual(len(r02), 1)

    def test_r02_valid_email_passes(self):
        record = self._make_record(email="user.name+tag@example.co.uk")
        issues = run_error_rules(record, "run1", set())
        r02 = [i for i in issues if i.rule_id == "R02_INVALID_EMAIL"]
        self.assertEqual(len(r02), 0)

    # Rule 3: Duplicate email
    def test_r03_duplicate_email(self):
        seen = {"john@example.com"}
        record = self._make_record(email="john@example.com")
        issues = run_error_rules(record, "run1", seen)
        r03 = [i for i in issues if i.rule_id == "R03_DUPLICATE_EMAIL"]
        self.assertEqual(len(r03), 1)

    def test_r03_duplicate_case_insensitive(self):
        seen = {"john@example.com"}
        record = self._make_record(email="JOHN@EXAMPLE.COM")
        issues = run_error_rules(record, "run1", seen)
        r03 = [i for i in issues if i.rule_id == "R03_DUPLICATE_EMAIL"]
        self.assertEqual(len(r03), 1)

    def test_r03_first_occurrence_passes(self):
        seen = set()
        record = self._make_record(email="john@example.com")
        issues = run_error_rules(record, "run1", seen)
        r03 = [i for i in issues if i.rule_id == "R03_DUPLICATE_EMAIL"]
        self.assertEqual(len(r03), 0)
        self.assertIn("john@example.com", seen)

    # Rule 4: Missing both names
    def test_r04_both_names_missing(self):
        record = self._make_record(first_name=None, last_name=None)
        issues = run_error_rules(record, "run1", set())
        r04 = [i for i in issues if i.rule_id == "R04_MISSING_BOTH_NAMES"]
        self.assertEqual(len(r04), 1)

    def test_r04_empty_strings_both_names(self):
        record = self._make_record(first_name="", last_name="")
        issues = run_error_rules(record, "run1", set())
        r04 = [i for i in issues if i.rule_id == "R04_MISSING_BOTH_NAMES"]
        self.assertEqual(len(r04), 1)

    def test_r04_first_name_only(self):
        record = self._make_record(first_name="John", last_name=None)
        issues = run_error_rules(record, "run1", set())
        r04 = [i for i in issues if i.rule_id == "R04_MISSING_BOTH_NAMES"]
        self.assertEqual(len(r04), 0)

    def test_r04_last_name_only(self):
        record = self._make_record(first_name=None, last_name="Doe")
        issues = run_error_rules(record, "run1", set())
        r04 = [i for i in issues if i.rule_id == "R04_MISSING_BOTH_NAMES"]
        self.assertEqual(len(r04), 0)

    # Rule 5: Future DOB
    def test_r05_future_dob(self):
        record = self._make_record(date_of_birth="2045-06-15")
        issues = run_error_rules(record, "run1", set())
        r05 = [i for i in issues if i.rule_id == "R05_FUTURE_DOB"]
        self.assertEqual(len(r05), 1)

    def test_r05_past_dob_ok(self):
        record = self._make_record(date_of_birth="1990-05-15")
        issues = run_error_rules(record, "run1", set())
        r05 = [i for i in issues if i.rule_id == "R05_FUTURE_DOB"]
        self.assertEqual(len(r05), 0)

    def test_r05_today_dob_not_future(self):
        # Born today is not in the future; it triggers COPPA though (age 0)
        record = self._make_record(date_of_birth="2026-04-10")
        issues = run_error_rules(record, "run1", set())
        r05 = [i for i in issues if i.rule_id == "R05_FUTURE_DOB"]
        self.assertEqual(len(r05), 0)

    # Rule 6: Under 13 (COPPA)
    def test_r06_exactly_13_passes(self):
        # Born 2013-04-10 is exactly 13 on 2026-04-10
        record = self._make_record(date_of_birth="2013-04-10")
        issues = run_error_rules(record, "run1", set())
        r06 = [i for i in issues if i.rule_id == "R06_UNDER_13_COPPA"]
        self.assertEqual(len(r06), 0)

    def test_r06_one_day_before_13th_birthday(self):
        # Born 2013-04-11 is still 12 on 2026-04-10
        record = self._make_record(date_of_birth="2013-04-11")
        issues = run_error_rules(record, "run1", set())
        r06 = [i for i in issues if i.rule_id == "R06_UNDER_13_COPPA"]
        self.assertEqual(len(r06), 1)

    def test_r06_age_12(self):
        record = self._make_record(date_of_birth="2014-06-01")
        issues = run_error_rules(record, "run1", set())
        r06 = [i for i in issues if i.rule_id == "R06_UNDER_13_COPPA"]
        self.assertEqual(len(r06), 1)

    # Rule 7: Over 120
    def test_r07_exactly_120_passes(self):
        # Born 1906-04-10 is exactly 120 on 2026-04-10
        record = self._make_record(date_of_birth="1906-04-10")
        issues = run_error_rules(record, "run1", set())
        r07 = [i for i in issues if i.rule_id == "R07_OVER_120"]
        self.assertEqual(len(r07), 0)

    def test_r07_age_121(self):
        # Born 1905-04-09 is 121 on 2026-04-10
        record = self._make_record(date_of_birth="1905-04-09")
        issues = run_error_rules(record, "run1", set())
        r07 = [i for i in issues if i.rule_id == "R07_OVER_120"]
        self.assertEqual(len(r07), 1)

    def test_r07_one_day_after_120th_birthday(self):
        # Born 1906-04-09 is just barely 120 (still 120)
        record = self._make_record(date_of_birth="1906-04-09")
        issues = run_error_rules(record, "run1", set())
        r07 = [i for i in issues if i.rule_id == "R07_OVER_120"]
        self.assertEqual(len(r07), 0)

    def test_r07_no_dob_no_error(self):
        record = self._make_record(date_of_birth=None)
        issues = run_error_rules(record, "run1", set())
        r07 = [i for i in issues if i.rule_id == "R07_OVER_120"]
        self.assertEqual(len(r07), 0)

    # Rule 8: Negative total_spend
    def test_r08_negative_spend(self):
        record = self._make_record(total_spend="-999.99")
        issues = run_error_rules(record, "run1", set())
        r08 = [i for i in issues if i.rule_id == "R08_NEGATIVE_SPEND"]
        self.assertEqual(len(r08), 1)

    def test_r08_zero_spend_ok(self):
        record = self._make_record(total_spend="0", num_orders="0")
        issues = run_error_rules(record, "run1", set())
        r08 = [i for i in issues if i.rule_id == "R08_NEGATIVE_SPEND"]
        self.assertEqual(len(r08), 0)

    def test_r08_positive_spend_ok(self):
        record = self._make_record(total_spend="500.00")
        issues = run_error_rules(record, "run1", set())
        r08 = [i for i in issues if i.rule_id == "R08_NEGATIVE_SPEND"]
        self.assertEqual(len(r08), 0)

    def test_r08_none_spend_ok(self):
        record = self._make_record(total_spend=None, num_orders=None)
        issues = run_error_rules(record, "run1", set())
        r08 = [i for i in issues if i.rule_id == "R08_NEGATIVE_SPEND"]
        self.assertEqual(len(r08), 0)

    # Rule 9: Invalid zip code
    def test_r09_invalid_zip_short(self):
        record = self._make_record(zip_code="1234")
        issues = run_error_rules(record, "run1", set())
        r09 = [i for i in issues if i.rule_id == "R09_INVALID_ZIP"]
        self.assertEqual(len(r09), 1)

    def test_r09_invalid_zip_hyphen(self):
        # "123-45" -> digits "12345" -> 5 digits -> valid after cleaning
        record = self._make_record(zip_code="123-45")
        issues = run_error_rules(record, "run1", set())
        r09 = [i for i in issues if i.rule_id == "R09_INVALID_ZIP"]
        self.assertEqual(len(r09), 0)

    def test_r09_valid_zip(self):
        record = self._make_record(zip_code="12345")
        issues = run_error_rules(record, "run1", set())
        r09 = [i for i in issues if i.rule_id == "R09_INVALID_ZIP"]
        self.assertEqual(len(r09), 0)

    def test_r09_zip_too_long(self):
        record = self._make_record(zip_code="123456")
        issues = run_error_rules(record, "run1", set())
        r09 = [i for i in issues if i.rule_id == "R09_INVALID_ZIP"]
        self.assertEqual(len(r09), 1)

    def test_r09_none_zip_ok(self):
        record = self._make_record(zip_code=None)
        issues = run_error_rules(record, "run1", set())
        r09 = [i for i in issues if i.rule_id == "R09_INVALID_ZIP"]
        self.assertEqual(len(r09), 0)

    # Rule 10: Missing signup_date
    def test_r10_missing_signup(self):
        record = self._make_record(signup_date=None)
        issues = run_error_rules(record, "run1", set())
        r10 = [i for i in issues if i.rule_id == "R10_MISSING_SIGNUP_DATE"]
        self.assertEqual(len(r10), 1)

    def test_r10_empty_signup(self):
        record = self._make_record(signup_date="")
        issues = run_error_rules(record, "run1", set())
        r10 = [i for i in issues if i.rule_id == "R10_MISSING_SIGNUP_DATE"]
        self.assertEqual(len(r10), 1)

    def test_r10_valid_signup(self):
        record = self._make_record(signup_date="2024-01-01")
        issues = run_error_rules(record, "run1", set())
        r10 = [i for i in issues if i.rule_id == "R10_MISSING_SIGNUP_DATE"]
        self.assertEqual(len(r10), 0)

    # Rule 11: Last order before signup
    def test_r11_order_before_signup(self):
        record = self._make_record(
            signup_date="2024-01-01",
            last_order_date="2023-06-15",
        )
        issues = run_error_rules(record, "run1", set())
        r11 = [i for i in issues if i.rule_id == "R11_ORDER_BEFORE_SIGNUP"]
        self.assertEqual(len(r11), 1)

    def test_r11_order_after_signup_ok(self):
        record = self._make_record(
            signup_date="2024-01-01",
            last_order_date="2024-06-15",
        )
        issues = run_error_rules(record, "run1", set())
        r11 = [i for i in issues if i.rule_id == "R11_ORDER_BEFORE_SIGNUP"]
        self.assertEqual(len(r11), 0)

    def test_r11_same_day_ok(self):
        record = self._make_record(
            signup_date="2024-01-01",
            last_order_date="2024-01-01",
        )
        issues = run_error_rules(record, "run1", set())
        r11 = [i for i in issues if i.rule_id == "R11_ORDER_BEFORE_SIGNUP"]
        self.assertEqual(len(r11), 0)

    def test_r11_no_last_order_ok(self):
        record = self._make_record(
            signup_date="2024-01-01",
            last_order_date=None,
        )
        issues = run_error_rules(record, "run1", set())
        r11 = [i for i in issues if i.rule_id == "R11_ORDER_BEFORE_SIGNUP"]
        self.assertEqual(len(r11), 0)

    # Rule 12: Future signup date
    def test_r12_future_signup(self):
        record = self._make_record(signup_date="2027-01-01")
        issues = run_error_rules(record, "run1", set())
        r12 = [i for i in issues if i.rule_id == "R12_FUTURE_SIGNUP"]
        self.assertEqual(len(r12), 1)

    def test_r12_today_signup_ok(self):
        record = self._make_record(signup_date="2026-04-10")
        issues = run_error_rules(record, "run1", set())
        r12 = [i for i in issues if i.rule_id == "R12_FUTURE_SIGNUP"]
        self.assertEqual(len(r12), 0)

    def test_r12_past_signup_ok(self):
        record = self._make_record(signup_date="2024-01-01")
        issues = run_error_rules(record, "run1", set())
        r12 = [i for i in issues if i.rule_id == "R12_FUTURE_SIGNUP"]
        self.assertEqual(len(r12), 0)

    # Rule 13: Future last order date
    def test_r13_future_order(self):
        record = self._make_record(
            signup_date="2024-01-01",
            last_order_date="2027-06-15",
        )
        issues = run_error_rules(record, "run1", set())
        r13 = [i for i in issues if i.rule_id == "R13_FUTURE_LAST_ORDER"]
        self.assertEqual(len(r13), 1)

    def test_r13_past_order_ok(self):
        record = self._make_record(
            signup_date="2024-01-01",
            last_order_date="2025-06-15",
        )
        issues = run_error_rules(record, "run1", set())
        r13 = [i for i in issues if i.rule_id == "R13_FUTURE_LAST_ORDER"]
        self.assertEqual(len(r13), 0)

    # Rule 14: Zero orders but positive spend
    def test_r14_zero_orders_positive_spend(self):
        record = self._make_record(num_orders="0", total_spend="500.00")
        issues = run_error_rules(record, "run1", set())
        r14 = [i for i in issues if i.rule_id == "R14_ZERO_ORDERS_POSITIVE_SPEND"]
        self.assertEqual(len(r14), 1)

    def test_r14_zero_orders_zero_spend_ok(self):
        record = self._make_record(num_orders="0", total_spend="0")
        issues = run_error_rules(record, "run1", set())
        r14 = [i for i in issues if i.rule_id == "R14_ZERO_ORDERS_POSITIVE_SPEND"]
        self.assertEqual(len(r14), 0)

    def test_r14_both_null_ok(self):
        record = self._make_record(num_orders=None, total_spend=None)
        issues = run_error_rules(record, "run1", set())
        r14 = [i for i in issues if i.rule_id == "R14_ZERO_ORDERS_POSITIVE_SPEND"]
        self.assertEqual(len(r14), 0)

    # Rule 15: Positive orders but zero/missing spend
    def test_r15_positive_orders_zero_spend(self):
        record = self._make_record(num_orders="5", total_spend="0")
        issues = run_error_rules(record, "run1", set())
        r15 = [i for i in issues if i.rule_id == "R15_POSITIVE_ORDERS_NO_SPEND"]
        self.assertEqual(len(r15), 1)

    def test_r15_positive_orders_none_spend(self):
        record = self._make_record(num_orders="5", total_spend=None)
        issues = run_error_rules(record, "run1", set())
        r15 = [i for i in issues if i.rule_id == "R15_POSITIVE_ORDERS_NO_SPEND"]
        self.assertEqual(len(r15), 1)

    def test_r15_positive_orders_positive_spend_ok(self):
        record = self._make_record(num_orders="5", total_spend="500.00")
        issues = run_error_rules(record, "run1", set())
        r15 = [i for i in issues if i.rule_id == "R15_POSITIVE_ORDERS_NO_SPEND"]
        self.assertEqual(len(r15), 0)


# ===================================================================
# WARNING rules (16-28)
# ===================================================================

class TestWarningRules(unittest.TestCase):
    """Tests for WARNING severity rules (16-28)."""

    def _make_record(self, **kwargs):
        defaults = {
            "row_number": 1,
            "customer_id": "CUST-TEST",
            "first_name": "John",
            "last_name": "Doe",
            "email": "john@example.com",
            "phone": "(555) 123-4567",
            "date_of_birth": "1990-01-15",
            "signup_date": "2024-01-01",
            "city": "Portland",
            "state": "OR",
            "zip_code": "97201",
            "loyalty_tier": "Gold",
            "total_spend": "500.00",
            "num_orders": "5",
            "last_order_date": "2024-06-15",
            "newsletter_opt_in": "True",
            "preferred_contact": "email",
        }
        defaults.update(kwargs)
        return RawCustomerRecord(**defaults)

    # Rule 16: Missing first name (but last name exists)
    def test_r16_missing_first_name(self):
        record = self._make_record(first_name=None, last_name="Doe")
        issues = run_warning_rules(record, "run1", {})
        r16 = [i for i in issues if i.rule_id == "R16_MISSING_FIRST_NAME"]
        self.assertEqual(len(r16), 1)

    def test_r16_both_present_ok(self):
        record = self._make_record(first_name="John", last_name="Doe")
        issues = run_warning_rules(record, "run1", {})
        r16 = [i for i in issues if i.rule_id == "R16_MISSING_FIRST_NAME"]
        self.assertEqual(len(r16), 0)

    def test_r16_both_missing_no_warning(self):
        # Both missing triggers an ERROR (R04), not this warning
        record = self._make_record(first_name=None, last_name=None)
        issues = run_warning_rules(record, "run1", {})
        r16 = [i for i in issues if i.rule_id == "R16_MISSING_FIRST_NAME"]
        self.assertEqual(len(r16), 0)

    # Rule 17: Missing last name (but first name exists)
    def test_r17_missing_last_name(self):
        record = self._make_record(first_name="John", last_name=None)
        issues = run_warning_rules(record, "run1", {})
        r17 = [i for i in issues if i.rule_id == "R17_MISSING_LAST_NAME"]
        self.assertEqual(len(r17), 1)

    def test_r17_both_present_ok(self):
        record = self._make_record(first_name="John", last_name="Doe")
        issues = run_warning_rules(record, "run1", {})
        r17 = [i for i in issues if i.rule_id == "R17_MISSING_LAST_NAME"]
        self.assertEqual(len(r17), 0)

    # Rule 18: Missing phone
    def test_r18_missing_phone(self):
        record = self._make_record(phone=None)
        issues = run_warning_rules(record, "run1", {})
        r18 = [i for i in issues if i.rule_id == "R18_MISSING_PHONE"]
        self.assertEqual(len(r18), 1)

    def test_r18_empty_phone(self):
        record = self._make_record(phone="")
        issues = run_warning_rules(record, "run1", {})
        r18 = [i for i in issues if i.rule_id == "R18_MISSING_PHONE"]
        self.assertEqual(len(r18), 1)

    def test_r18_phone_present_ok(self):
        record = self._make_record(phone="(555) 123-4567")
        issues = run_warning_rules(record, "run1", {})
        r18 = [i for i in issues if i.rule_id == "R18_MISSING_PHONE"]
        self.assertEqual(len(r18), 0)

    # Rule 19: Missing DOB
    def test_r19_missing_dob(self):
        record = self._make_record(date_of_birth=None)
        issues = run_warning_rules(record, "run1", {})
        r19 = [i for i in issues if i.rule_id == "R19_MISSING_DOB"]
        self.assertEqual(len(r19), 1)

    def test_r19_dob_present_ok(self):
        record = self._make_record(date_of_birth="1990-01-15")
        issues = run_warning_rules(record, "run1", {})
        r19 = [i for i in issues if i.rule_id == "R19_MISSING_DOB"]
        self.assertEqual(len(r19), 0)

    # Rule 20: Missing loyalty_tier
    def test_r20_missing_tier(self):
        record = self._make_record(loyalty_tier=None)
        issues = run_warning_rules(record, "run1", {})
        r20 = [i for i in issues if i.rule_id == "R20_MISSING_LOYALTY_TIER"]
        self.assertEqual(len(r20), 1)

    def test_r20_tier_present_ok(self):
        record = self._make_record(loyalty_tier="Gold")
        issues = run_warning_rules(record, "run1", {})
        r20 = [i for i in issues if i.rule_id == "R20_MISSING_LOYALTY_TIER"]
        self.assertEqual(len(r20), 0)

    # Rule 21: Missing preferred_contact
    def test_r21_missing_contact(self):
        record = self._make_record(preferred_contact=None)
        issues = run_warning_rules(record, "run1", {})
        r21 = [i for i in issues if i.rule_id == "R21_MISSING_PREFERRED_CONTACT"]
        self.assertEqual(len(r21), 1)

    def test_r21_contact_present_ok(self):
        record = self._make_record(preferred_contact="email")
        issues = run_warning_rules(record, "run1", {})
        r21 = [i for i in issues if i.rule_id == "R21_MISSING_PREFERRED_CONTACT"]
        self.assertEqual(len(r21), 0)

    # Rule 22: Name contains digits or special chars
    def test_r22_name_with_digits(self):
        record = self._make_record(first_name="John3")
        issues = run_warning_rules(record, "run1", {})
        r22 = [i for i in issues if i.rule_id == "R22_NAME_SUSPICIOUS_CHARS"]
        self.assertEqual(len(r22), 1)

    def test_r22_name_with_special_chars(self):
        record = self._make_record(last_name="Doe@!")
        issues = run_warning_rules(record, "run1", {})
        r22 = [i for i in issues if i.rule_id == "R22_NAME_SUSPICIOUS_CHARS"]
        self.assertEqual(len(r22), 1)

    def test_r22_name_with_apostrophe_ok(self):
        record = self._make_record(last_name="O'Brien")
        issues = run_warning_rules(record, "run1", {})
        r22 = [i for i in issues if i.rule_id == "R22_NAME_SUSPICIOUS_CHARS"]
        self.assertEqual(len(r22), 0)

    def test_r22_name_with_hyphen_ok(self):
        record = self._make_record(last_name="Smith-Jones")
        issues = run_warning_rules(record, "run1", {})
        r22 = [i for i in issues if i.rule_id == "R22_NAME_SUSPICIOUS_CHARS"]
        self.assertEqual(len(r22), 0)

    def test_r22_unicode_name(self):
        # Unicode letters should trigger the regex since it only allows [a-zA-Z...]
        record = self._make_record(first_name="Jose\u0301")
        issues = run_warning_rules(record, "run1", {})
        r22 = [i for i in issues if i.rule_id == "R22_NAME_SUSPICIOUS_CHARS"]
        # The combining accent (U+0301) is not in [a-zA-Z\s\-'\.], so it flags
        self.assertEqual(len(r22), 1)

    # Rule 23: Single-character name
    def test_r23_single_char_first_name(self):
        record = self._make_record(first_name="J")
        issues = run_warning_rules(record, "run1", {})
        r23 = [i for i in issues if i.rule_id == "R23_SINGLE_CHAR_NAME"]
        self.assertEqual(len(r23), 1)

    def test_r23_single_char_last_name(self):
        record = self._make_record(last_name="D")
        issues = run_warning_rules(record, "run1", {})
        r23 = [i for i in issues if i.rule_id == "R23_SINGLE_CHAR_NAME"]
        self.assertEqual(len(r23), 1)

    def test_r23_two_char_name_ok(self):
        record = self._make_record(first_name="Jo")
        issues = run_warning_rules(record, "run1", {})
        r23 = [i for i in issues if i.rule_id == "R23_SINGLE_CHAR_NAME"]
        self.assertEqual(len(r23), 0)

    # Rule 24: City contains digits
    def test_r24_city_with_digits(self):
        record = self._make_record(city="Springfield2")
        issues = run_warning_rules(record, "run1", {})
        r24 = [i for i in issues if i.rule_id == "R24_CITY_HAS_DIGITS"]
        self.assertEqual(len(r24), 1)

    def test_r24_normal_city_ok(self):
        record = self._make_record(city="Portland")
        issues = run_warning_rules(record, "run1", {})
        r24 = [i for i in issues if i.rule_id == "R24_CITY_HAS_DIGITS"]
        self.assertEqual(len(r24), 0)

    # Rule 25: Tier-spend mismatch
    def test_r25_platinum_low_spend(self):
        record = self._make_record(loyalty_tier="Platinum", total_spend="50.00")
        issues = run_warning_rules(record, "run1", {})
        r25 = [i for i in issues if i.rule_id == "R25_TIER_SPEND_MISMATCH"]
        self.assertEqual(len(r25), 1)

    def test_r25_bronze_high_spend(self):
        record = self._make_record(loyalty_tier="Bronze", total_spend="5000.00")
        issues = run_warning_rules(record, "run1", {})
        r25 = [i for i in issues if i.rule_id == "R25_TIER_SPEND_MISMATCH"]
        self.assertEqual(len(r25), 1)

    def test_r25_gold_normal_spend_ok(self):
        record = self._make_record(loyalty_tier="Gold", total_spend="2000.00")
        issues = run_warning_rules(record, "run1", {})
        r25 = [i for i in issues if i.rule_id == "R25_TIER_SPEND_MISMATCH"]
        self.assertEqual(len(r25), 0)

    # Rule 26: AOV outlier
    def test_r26_aov_too_high(self):
        # $5000 / 2 orders = $2500 AOV (> $1000 max)
        record = self._make_record(total_spend="5000.00", num_orders="2")
        issues = run_warning_rules(record, "run1", {})
        r26 = [i for i in issues if i.rule_id == "R26_AOV_OUTLIER"]
        self.assertEqual(len(r26), 1)

    def test_r26_aov_too_low(self):
        # $0.50 / 1 order = $0.50 AOV (< $1 min)
        record = self._make_record(total_spend="0.50", num_orders="1")
        issues = run_warning_rules(record, "run1", {})
        r26 = [i for i in issues if i.rule_id == "R26_AOV_OUTLIER"]
        self.assertEqual(len(r26), 1)

    def test_r26_aov_normal_ok(self):
        # $500 / 5 orders = $100 AOV (in range)
        record = self._make_record(total_spend="500.00", num_orders="5")
        issues = run_warning_rules(record, "run1", {})
        r26 = [i for i in issues if i.rule_id == "R26_AOV_OUTLIER"]
        self.assertEqual(len(r26), 0)

    def test_r26_zero_orders_skipped(self):
        # Zero orders: AOV not computed
        record = self._make_record(total_spend="500.00", num_orders="0")
        issues = run_warning_rules(record, "run1", {})
        r26 = [i for i in issues if i.rule_id == "R26_AOV_OUTLIER"]
        self.assertEqual(len(r26), 0)

    # Rule 27: Phone missing area code
    def test_r27_seven_digit_phone(self):
        record = self._make_record(phone="1234567")
        issues = run_warning_rules(record, "run1", {})
        r27 = [i for i in issues if i.rule_id == "R27_PHONE_MISSING_AREA_CODE"]
        self.assertEqual(len(r27), 1)

    def test_r27_ten_digit_phone_ok(self):
        record = self._make_record(phone="(555) 123-4567")
        issues = run_warning_rules(record, "run1", {})
        r27 = [i for i in issues if i.rule_id == "R27_PHONE_MISSING_AREA_CODE"]
        self.assertEqual(len(r27), 0)

    # Rule 28: Fuzzy duplicate
    def test_r28_fuzzy_duplicate_detected(self):
        fuzzy_dupes = {}
        # First record sets up the fuzzy key
        record1 = self._make_record(
            row_number=1, last_name="Smith", zip_code="12345",
            email="smith1@example.com",
        )
        run_warning_rules(record1, "run1", fuzzy_dupes)

        # Second record with same last name + zip but different email
        record2 = self._make_record(
            row_number=2, last_name="Smith", zip_code="12345",
            email="smith2@example.com",
        )
        issues = run_warning_rules(record2, "run1", fuzzy_dupes)
        r28 = [i for i in issues if i.rule_id == "R28_FUZZY_DUPLICATE"]
        self.assertEqual(len(r28), 1)

    def test_r28_same_email_no_fuzzy_dupe(self):
        fuzzy_dupes = {}
        record1 = self._make_record(
            row_number=1, last_name="Smith", zip_code="12345",
            email="smith@example.com",
        )
        run_warning_rules(record1, "run1", fuzzy_dupes)

        # Same email, same name, same zip: not a fuzzy dupe
        record2 = self._make_record(
            row_number=2, last_name="Smith", zip_code="12345",
            email="smith@example.com",
        )
        issues = run_warning_rules(record2, "run1", fuzzy_dupes)
        r28 = [i for i in issues if i.rule_id == "R28_FUZZY_DUPLICATE"]
        self.assertEqual(len(r28), 0)

    def test_r28_missing_last_name_skipped(self):
        fuzzy_dupes = {}
        record = self._make_record(last_name=None, zip_code="12345")
        issues = run_warning_rules(record, "run1", fuzzy_dupes)
        r28 = [i for i in issues if i.rule_id == "R28_FUZZY_DUPLICATE"]
        self.assertEqual(len(r28), 0)

    def test_r28_missing_zip_skipped(self):
        fuzzy_dupes = {}
        record = self._make_record(last_name="Smith", zip_code=None)
        issues = run_warning_rules(record, "run1", fuzzy_dupes)
        r28 = [i for i in issues if i.rule_id == "R28_FUZZY_DUPLICATE"]
        self.assertEqual(len(r28), 0)


# ===================================================================
# Full validate_record integration
# ===================================================================

class TestValidateRecord(unittest.TestCase):
    """Integration tests for the validate_record function."""

    def _make_record(self, **kwargs):
        defaults = {
            "row_number": 1,
            "customer_id": "CUST-TEST",
            "first_name": "John",
            "last_name": "Doe",
            "email": "john@example.com",
            "phone": "(555) 123-4567",
            "date_of_birth": "1990-01-15",
            "signup_date": "2024-01-01",
            "city": "Portland",
            "state": "OR",
            "zip_code": "97201",
            "loyalty_tier": "Gold",
            "total_spend": "500.00",
            "num_orders": "5",
            "last_order_date": "2024-06-15",
            "newsletter_opt_in": "True",
            "preferred_contact": "email",
        }
        defaults.update(kwargs)
        return RawCustomerRecord(**defaults)

    def test_valid_record_no_errors(self):
        record = self._make_record()
        meta = {"whitespace_fields": [], "null_sentinel_fields": []}
        _, issues, has_errors = validate_record(
            record, "run1", meta, set(), {},
        )
        error_issues = [i for i in issues if i.severity == Severity.ERROR]
        self.assertFalse(has_errors)
        self.assertEqual(len(error_issues), 0)

    def test_record_with_cleaning_and_no_errors(self):
        record = self._make_record(
            state="california",
            loyalty_tier="SILVER",
            newsletter_opt_in="yes",
        )
        meta = {"whitespace_fields": [], "null_sentinel_fields": []}
        cleaned, issues, has_errors = validate_record(
            record, "run1", meta, set(), {},
        )
        self.assertFalse(has_errors)
        # Should have cleaning issues
        cleaned_issues = [i for i in issues if i.severity == Severity.CLEANED]
        self.assertGreater(len(cleaned_issues), 0)
        # Verify cleaned values
        self.assertEqual(cleaned.state, "CA")
        self.assertEqual(cleaned.loyalty_tier, "Silver")
        self.assertEqual(cleaned.newsletter_opt_in, "True")

    def test_record_with_error_flagged(self):
        record = self._make_record(email=None)
        meta = {"whitespace_fields": [], "null_sentinel_fields": []}
        _, issues, has_errors = validate_record(
            record, "run1", meta, set(), {},
        )
        self.assertTrue(has_errors)
        error_issues = [i for i in issues if i.severity == Severity.ERROR]
        self.assertGreater(len(error_issues), 0)

    def test_today_constant_is_april_10_2026(self):
        """Verify the hardcoded TODAY date matches test expectations."""
        self.assertEqual(TODAY, date(2026, 4, 10))


if __name__ == "__main__":
    unittest.main()
