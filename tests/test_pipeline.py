"""
Integration tests for the full pipeline.

Creates a small synthetic CSV (15 rows) with one example of each major
issue type. Runs the full pipeline, then verifies:
- Correct records land in SQLite
- Rejected records appear in the validation report
- Cleaning actions are logged
- Row counts: clean + rejected = total (no silent drops)
"""

import json
import os
import sqlite3
import tempfile
import unittest


# Synthetic CSV data: 15 rows covering various issue types.
# Row layout:
#   1. Clean record (no issues)
#   2. Missing email (R01 ERROR)
#   3. Double @@ email (R02 ERROR)
#   4. Duplicate email of row 1 (R03 ERROR)
#   5. Missing both names (R04 ERROR)
#   6. Future DOB (R05 ERROR)
#   7. Negative spend (R08 ERROR)
#   8. Invalid zip (R09 ERROR)
#   9. Missing signup date (R10 ERROR)
#  10. Last order before signup (R11 ERROR)
#  11. Clean record with missing phone (R18 WARNING)
#  12. Clean record with state to normalize (R30 CLEANED)
#  13. Clean record with phone to normalize (R31 CLEANED)
#  14. Clean record with tier to normalize (R32 CLEANED)
#  15. Clean record with newsletter to normalize (R33 CLEANED)

SYNTHETIC_CSV = """\
customer_id,first_name,last_name,email,phone,date_of_birth,signup_date,city,state,zip_code,loyalty_tier,total_spend,num_orders,last_order_date,newsletter_opt_in,preferred_contact,notes
CUST-0001,Alice,Smith,alice@example.com,(555) 111-1111,1990-01-15,2020-01-01,Portland,OR,97201,Gold,500.00,5,2024-06-15,True,email,
CUST-0002,Bob,Jones,,5551112222,1985-03-20,2021-05-10,Seattle,WA,98101,Silver,300.00,3,2023-12-01,True,phone,
CUST-0003,Carol,White,carol@@example.com,(555) 333-3333,1978-07-04,2019-11-15,Denver,CO,80201,Bronze,150.00,2,2023-01-10,False,email,
CUST-0004,Dave,Brown,alice@example.com,(555) 444-4444,1992-09-30,2022-03-01,Austin,TX,73301,Gold,800.00,8,2024-01-20,True,mail,
CUST-0005,,,noname@example.com,(555) 555-5555,1980-12-01,2018-06-01,Chicago,IL,60601,Silver,200.00,2,2022-08-15,True,phone,
CUST-0006,Eve,Future,eve@example.com,(555) 666-6666,2045-06-15,2023-01-01,Miami,FL,33101,Bronze,100.00,1,2024-02-28,False,email,
CUST-0007,Frank,Negative,frank@example.com,(555) 777-7777,1975-11-20,2020-07-15,Boston,MA,02101,Gold,-999.99,10,2024-03-01,True,phone,
CUST-0008,Grace,Badzip,grace@example.com,(555) 888-8888,1988-04-22,2021-09-01,Dallas,TX,1234,Silver,400.00,4,2023-11-30,True,mail,
CUST-0009,Hank,Nosignup,hank@example.com,(555) 999-0000,1995-02-14,,Houston,TX,77001,Bronze,50.00,1,2022-05-10,False,email,
CUST-0010,Ivy,Timeline,ivy@example.com,(555) 000-1111,1983-08-30,2023-06-01,Phoenix,AZ,85001,Gold,600.00,6,2020-01-15,True,phone,
CUST-0011,Jack,Nophone,jack@example.com,,1991-05-05,2022-01-15,Atlanta,GA,30301,Silver,250.00,3,2023-07-20,True,email,
CUST-0012,Kate,Statefix,kate@example.com,(555) 222-3333,1987-10-12,2021-04-01,Sacramento,california,95814,Gold,700.00,7,2024-04-01,True,mail,
CUST-0013,Leo,Phonefix,leo@example.com,555.333.4444,1979-01-25,2020-11-01,Columbus,OH,43201,Silver,350.00,4,2023-09-15,False,phone,
CUST-0014,Mia,Tierfix,mia@example.com,(555) 444-5555,1993-06-18,2023-02-01,Charlotte,NC,28201,PLATINUM,900.00,9,2024-05-01,True,email,
CUST-0015,Nate,Newsletter,nate@example.com,(555) 555-6666,1986-03-07,2019-08-01,Nashville,TN,37201,Bronze,120.00,2,2023-03-10,yes,phone,
"""


class TestPipelineIntegration(unittest.TestCase):
    """Integration tests running the full pipeline on synthetic data."""

    @classmethod
    def setUpClass(cls):
        """Create temp directory, write synthetic CSV, and run pipeline."""
        cls.tmpdir = tempfile.mkdtemp(prefix="pipeline_test_")
        cls.csv_path = os.path.join(cls.tmpdir, "test_customers.csv")
        cls.db_path = os.path.join(cls.tmpdir, "test_output.db")
        cls.report_path = os.path.join(cls.tmpdir, "test_report.json")

        # Write the synthetic CSV
        with open(cls.csv_path, "w", encoding="utf-8") as f:
            f.write(SYNTHETIC_CSV)

        # Import and run the pipeline
        from pipeline import run_pipeline
        cls.result = run_pipeline(
            input_file=cls.csv_path,
            output_db=cls.db_path,
            report_path=cls.report_path,
            dry_run=False,
            verbose=False,
        )

        # Load the report for inspection
        with open(cls.report_path, "r", encoding="utf-8") as f:
            cls.report = json.load(f)

    @classmethod
    def tearDownClass(cls):
        """Clean up temp directory."""
        import shutil
        shutil.rmtree(cls.tmpdir, ignore_errors=True)

    # -----------------------------------------------------------------
    # Row count invariants
    # -----------------------------------------------------------------

    def test_total_records_loaded(self):
        """Verify the pipeline found all 15 rows."""
        self.assertEqual(self.result["total_records"], 15)

    def test_no_silent_drops(self):
        """clean + rejected = total (no records silently dropped)."""
        total = self.result["total_records"]
        clean = self.result["clean_records"]
        rejected = self.result["rejected_records"]
        self.assertEqual(clean + rejected, total)

    # -----------------------------------------------------------------
    # SQLite verification
    # -----------------------------------------------------------------

    def test_sqlite_db_exists(self):
        self.assertTrue(os.path.exists(self.db_path))

    def test_sqlite_customer_count(self):
        """Clean records in SQLite should match the pipeline result."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM customers")
        count = cursor.fetchone()[0]
        conn.close()
        self.assertEqual(count, self.result["clean_records"])

    def test_sqlite_clean_record_present(self):
        """The known-clean CUST-0001 should be in the database."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute(
            "SELECT email FROM customers WHERE customer_id = 'CUST-0001'"
        )
        row = cursor.fetchone()
        conn.close()
        self.assertIsNotNone(row)
        self.assertEqual(row[0], "alice@example.com")

    def test_sqlite_rejected_record_absent(self):
        """CUST-0002 (missing email) should not be in the database."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute(
            "SELECT COUNT(*) FROM customers WHERE customer_id = 'CUST-0002'"
        )
        count = cursor.fetchone()[0]
        conn.close()
        self.assertEqual(count, 0)

    def test_sqlite_pipeline_run_exists(self):
        """pipeline_runs table should have exactly one entry."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM pipeline_runs")
        count = cursor.fetchone()[0]
        conn.close()
        self.assertEqual(count, 1)

    # -----------------------------------------------------------------
    # Validation report verification
    # -----------------------------------------------------------------

    def test_report_file_exists(self):
        self.assertTrue(os.path.exists(self.report_path))

    def test_report_has_issues(self):
        """The report should contain validation issues."""
        self.assertGreater(len(self.report["issues"]), 0)

    def test_report_contains_error_issues(self):
        """Report should contain ERROR severity issues for rejected records."""
        error_issues = [
            i for i in self.report["issues"] if i["severity"] == "ERROR"
        ]
        self.assertGreater(len(error_issues), 0)

    def test_report_contains_missing_email_error(self):
        """Row 2 (CUST-0002) should have a missing email error."""
        r01_issues = [
            i for i in self.report["issues"]
            if i["rule_id"] == "R01_MISSING_EMAIL"
        ]
        self.assertGreater(len(r01_issues), 0)
        cust_0002 = [i for i in r01_issues if i["customer_id"] == "CUST-0002"]
        self.assertEqual(len(cust_0002), 1)

    def test_report_contains_double_at_error(self):
        """Row 3 (CUST-0003) should have a double @@ email error."""
        r02_issues = [
            i for i in self.report["issues"]
            if i["rule_id"] == "R02_INVALID_EMAIL"
            and i["customer_id"] == "CUST-0003"
        ]
        self.assertGreater(len(r02_issues), 0)

    def test_report_contains_duplicate_email_error(self):
        """Row 4 (CUST-0004) should have a duplicate email error."""
        r03_issues = [
            i for i in self.report["issues"]
            if i["rule_id"] == "R03_DUPLICATE_EMAIL"
            and i["customer_id"] == "CUST-0004"
        ]
        self.assertGreater(len(r03_issues), 0)

    def test_report_contains_future_dob_error(self):
        """Row 6 (CUST-0006) should have a future DOB error."""
        r05_issues = [
            i for i in self.report["issues"]
            if i["rule_id"] == "R05_FUTURE_DOB"
            and i["customer_id"] == "CUST-0006"
        ]
        self.assertGreater(len(r05_issues), 0)

    def test_report_contains_negative_spend_error(self):
        """Row 7 (CUST-0007) should have a negative spend error."""
        r08_issues = [
            i for i in self.report["issues"]
            if i["rule_id"] == "R08_NEGATIVE_SPEND"
            and i["customer_id"] == "CUST-0007"
        ]
        self.assertGreater(len(r08_issues), 0)

    # -----------------------------------------------------------------
    # Cleaning actions verification
    # -----------------------------------------------------------------

    def test_cleaning_actions_logged(self):
        """Pipeline should have logged cleaning actions."""
        self.assertGreater(self.result["cleaning_actions"], 0)

    def test_report_contains_cleaned_issues(self):
        """Report should contain CLEANED severity issues."""
        cleaned_issues = [
            i for i in self.report["issues"] if i["severity"] == "CLEANED"
        ]
        self.assertGreater(len(cleaned_issues), 0)

    def test_state_normalization_logged(self):
        """CUST-0012 should have a state normalization entry."""
        r30_issues = [
            i for i in self.report["issues"]
            if i["rule_id"] == "R30_STATE_NORMALIZE"
            and i["customer_id"] == "CUST-0012"
        ]
        self.assertGreater(len(r30_issues), 0)

    def test_phone_normalization_logged(self):
        """CUST-0013 should have a phone normalization entry."""
        r31_issues = [
            i for i in self.report["issues"]
            if i["rule_id"] == "R31_PHONE_NORMALIZE"
            and i["customer_id"] == "CUST-0013"
        ]
        self.assertGreater(len(r31_issues), 0)

    def test_tier_normalization_logged(self):
        """CUST-0014 should have a tier normalization entry."""
        r32_issues = [
            i for i in self.report["issues"]
            if i["rule_id"] == "R32_TIER_NORMALIZE"
            and i["customer_id"] == "CUST-0014"
        ]
        self.assertGreater(len(r32_issues), 0)

    def test_newsletter_normalization_logged(self):
        """CUST-0015 should have a newsletter normalization entry."""
        r33_issues = [
            i for i in self.report["issues"]
            if i["rule_id"] == "R33_NEWSLETTER_NORMALIZE"
            and i["customer_id"] == "CUST-0015"
        ]
        self.assertGreater(len(r33_issues), 0)

    # -----------------------------------------------------------------
    # Warning verification
    # -----------------------------------------------------------------

    def test_missing_phone_warning(self):
        """CUST-0011 should have a missing phone warning."""
        r18_issues = [
            i for i in self.report["issues"]
            if i["rule_id"] == "R18_MISSING_PHONE"
            and i["customer_id"] == "CUST-0011"
        ]
        self.assertGreater(len(r18_issues), 0)

    # -----------------------------------------------------------------
    # SQLite schema verification
    # -----------------------------------------------------------------

    def test_sqlite_tables_exist(self):
        """All three expected tables should exist."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute(
            "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
        )
        tables = [row[0] for row in cursor.fetchall()]
        conn.close()
        self.assertIn("customers", tables)
        self.assertIn("pipeline_runs", tables)
        self.assertIn("validation_issues", tables)

    def test_validation_issues_table_populated(self):
        """validation_issues table should have entries."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM validation_issues")
        count = cursor.fetchone()[0]
        conn.close()
        self.assertGreater(count, 0)

    # -----------------------------------------------------------------
    # Report summary consistency
    # -----------------------------------------------------------------

    def test_report_summary_matches_result(self):
        """Report summary counts should match pipeline result dict."""
        summary = self.report["summary"]
        self.assertEqual(summary["total_records"], self.result["total_records"])
        self.assertEqual(summary["clean_records"], self.result["clean_records"])
        self.assertEqual(summary["rejected_records"], self.result["rejected_records"])


if __name__ == "__main__":
    unittest.main()
