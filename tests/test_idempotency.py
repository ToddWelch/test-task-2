"""
Idempotency tests for the customer data pipeline.

Runs the pipeline twice on the same input file and verifies:
- SQLite customer rows are identical between runs
- Validation report issues are identical (except run_id and timestamp)
- pipeline_runs table shows the same input_hash but different run_ids
"""

import json
import os
import shutil
import sqlite3
import tempfile
import unittest


class TestIdempotency(unittest.TestCase):
    """
    Run the pipeline twice on the same CSV and verify identical outputs.
    Uses data/customers.csv as the input file (the real dataset).
    """

    @classmethod
    def setUpClass(cls):
        """Run the pipeline twice, storing outputs in separate temp dirs."""
        cls.project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        cls.csv_path = os.path.join(cls.project_root, "data", "customers.csv")

        if not os.path.exists(cls.csv_path):
            raise unittest.SkipTest(f"Input file not found: {cls.csv_path}")

        cls.tmpdir = tempfile.mkdtemp(prefix="idempotency_test_")

        # Run 1
        cls.db_path_1 = os.path.join(cls.tmpdir, "run1.db")
        cls.report_path_1 = os.path.join(cls.tmpdir, "run1_report.json")

        # Run 2
        cls.db_path_2 = os.path.join(cls.tmpdir, "run2.db")
        cls.report_path_2 = os.path.join(cls.tmpdir, "run2_report.json")

        from pipeline import run_pipeline

        cls.result_1 = run_pipeline(
            input_file=cls.csv_path,
            output_db=cls.db_path_1,
            report_path=cls.report_path_1,
            dry_run=False,
            verbose=False,
        )

        cls.result_2 = run_pipeline(
            input_file=cls.csv_path,
            output_db=cls.db_path_2,
            report_path=cls.report_path_2,
            dry_run=False,
            verbose=False,
        )

        # Load reports
        with open(cls.report_path_1, "r", encoding="utf-8") as f:
            cls.report_1 = json.load(f)
        with open(cls.report_path_2, "r", encoding="utf-8") as f:
            cls.report_2 = json.load(f)

    @classmethod
    def tearDownClass(cls):
        """Clean up temp directory."""
        shutil.rmtree(cls.tmpdir, ignore_errors=True)

    # -----------------------------------------------------------------
    # Pipeline result counts must be identical
    # -----------------------------------------------------------------

    def test_same_total_records(self):
        self.assertEqual(
            self.result_1["total_records"],
            self.result_2["total_records"],
        )

    def test_same_clean_records(self):
        self.assertEqual(
            self.result_1["clean_records"],
            self.result_2["clean_records"],
        )

    def test_same_rejected_records(self):
        self.assertEqual(
            self.result_1["rejected_records"],
            self.result_2["rejected_records"],
        )

    def test_same_warning_records(self):
        self.assertEqual(
            self.result_1["warning_records"],
            self.result_2["warning_records"],
        )

    def test_same_cleaning_actions(self):
        self.assertEqual(
            self.result_1["cleaning_actions"],
            self.result_2["cleaning_actions"],
        )

    # -----------------------------------------------------------------
    # SQLite customer data must be identical
    # -----------------------------------------------------------------

    def _get_customer_rows(self, db_path):
        """
        Fetch all customer rows sorted by customer_id.
        Excludes created_at and updated_at since they contain timestamps.
        """
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute("""
            SELECT customer_id, first_name, last_name, email, phone,
                   date_of_birth, signup_date, city, state, zip_code,
                   loyalty_tier, total_spend, num_orders, last_order_date,
                   newsletter_opt_in, preferred_contact, notes
            FROM customers
            ORDER BY customer_id
        """)
        rows = cursor.fetchall()
        conn.close()
        return rows

    def test_customer_rows_identical(self):
        """All customer data (excluding timestamps) should match."""
        rows_1 = self._get_customer_rows(self.db_path_1)
        rows_2 = self._get_customer_rows(self.db_path_2)
        self.assertEqual(len(rows_1), len(rows_2))
        for r1, r2 in zip(rows_1, rows_2):
            self.assertEqual(r1, r2)

    def test_customer_count_match(self):
        """Both runs should produce the same number of clean records."""
        rows_1 = self._get_customer_rows(self.db_path_1)
        rows_2 = self._get_customer_rows(self.db_path_2)
        self.assertEqual(len(rows_1), len(rows_2))

    # -----------------------------------------------------------------
    # Validation report must be identical (except run_id/timestamp)
    # -----------------------------------------------------------------

    def _strip_run_metadata(self, report):
        """
        Return a copy of the report with run_id and run_timestamp
        removed so we can compare everything else.
        """
        issues = []
        for issue in report["issues"]:
            stripped = {
                k: v for k, v in issue.items()
                if k != "run_id"
            }
            issues.append(stripped)
        return sorted(issues, key=lambda x: (
            x.get("row_number", 0) or 0,
            x.get("rule_id", ""),
        ))

    def test_report_issues_identical(self):
        """Issues (minus run_id) should be identical between runs."""
        issues_1 = self._strip_run_metadata(self.report_1)
        issues_2 = self._strip_run_metadata(self.report_2)
        self.assertEqual(len(issues_1), len(issues_2))
        for i1, i2 in zip(issues_1, issues_2):
            self.assertEqual(i1, i2)

    def test_report_summary_counts_identical(self):
        """Summary counts should be identical (excluding run_id/timestamp)."""
        s1 = self.report_1["summary"]
        s2 = self.report_2["summary"]
        for key in [
            "total_records", "clean_records", "rejected_records",
            "warning_records", "cleaning_actions", "total_issues",
            "error_count", "warning_count", "cleaned_count",
        ]:
            self.assertEqual(s1[key], s2[key], f"Mismatch on {key}")

    # -----------------------------------------------------------------
    # pipeline_runs table: same hash, different run_id
    # -----------------------------------------------------------------

    def _get_pipeline_run(self, db_path):
        """Get the pipeline_runs row from the database."""
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT run_id, input_hash FROM pipeline_runs")
        row = cursor.fetchone()
        conn.close()
        return row

    def test_same_input_hash(self):
        """Both runs should produce the same SHA256 hash of the input."""
        run1 = self._get_pipeline_run(self.db_path_1)
        run2 = self._get_pipeline_run(self.db_path_2)
        self.assertEqual(run1[1], run2[1])  # input_hash

    def test_different_run_ids(self):
        """Each run should get a unique run_id."""
        run1 = self._get_pipeline_run(self.db_path_1)
        run2 = self._get_pipeline_run(self.db_path_2)
        self.assertNotEqual(run1[0], run2[0])  # run_id

    # -----------------------------------------------------------------
    # No silent drops in either run
    # -----------------------------------------------------------------

    def test_no_silent_drops_run1(self):
        total = self.result_1["total_records"]
        clean = self.result_1["clean_records"]
        rejected = self.result_1["rejected_records"]
        self.assertEqual(clean + rejected, total)

    def test_no_silent_drops_run2(self):
        total = self.result_2["total_records"]
        clean = self.result_2["clean_records"]
        rejected = self.result_2["rejected_records"]
        self.assertEqual(clean + rejected, total)


if __name__ == "__main__":
    unittest.main()
