"""
SQLite writer for the customer data pipeline.

Creates the database with three tables (customers, pipeline_runs,
validation_issues), inserts clean records, pipeline run metadata,
and all validation issues. Uses DROP TABLE IF EXISTS for idempotency.
All operations run within a single transaction.
"""

import sqlite3
from pathlib import Path
from typing import Optional

from src.models import CustomerRecord, PipelineRun, ValidationIssue


# ---------------------------------------------------------------------------
# SQL statements
# ---------------------------------------------------------------------------

DROP_CUSTOMERS = "DROP TABLE IF EXISTS customers"
DROP_PIPELINE_RUNS = "DROP TABLE IF EXISTS pipeline_runs"
DROP_VALIDATION_ISSUES = "DROP TABLE IF EXISTS validation_issues"

CREATE_CUSTOMERS = """
CREATE TABLE customers (
    customer_id TEXT PRIMARY KEY,
    first_name TEXT,
    last_name TEXT,
    email TEXT NOT NULL UNIQUE,
    phone TEXT,
    date_of_birth TEXT,
    signup_date TEXT NOT NULL,
    city TEXT,
    state TEXT,
    zip_code TEXT,
    loyalty_tier TEXT,
    total_spend REAL,
    num_orders INTEGER,
    last_order_date TEXT,
    newsletter_opt_in INTEGER,
    preferred_contact TEXT,
    notes TEXT,
    created_at TEXT DEFAULT (datetime('now')),
    updated_at TEXT DEFAULT (datetime('now'))
)
"""

CREATE_PIPELINE_RUNS = """
CREATE TABLE pipeline_runs (
    run_id TEXT PRIMARY KEY,
    run_timestamp TEXT NOT NULL,
    input_file TEXT NOT NULL,
    input_hash TEXT NOT NULL,
    total_records INTEGER,
    clean_records INTEGER,
    rejected_records INTEGER,
    warning_records INTEGER,
    cleaning_actions INTEGER
)
"""

CREATE_VALIDATION_ISSUES = """
CREATE TABLE validation_issues (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT NOT NULL,
    customer_id TEXT,
    row_number INTEGER,
    field TEXT,
    rule_id TEXT,
    severity TEXT,
    message TEXT,
    original_value TEXT,
    corrected_value TEXT,
    FOREIGN KEY (run_id) REFERENCES pipeline_runs(run_id)
)
"""

INSERT_CUSTOMER = """
INSERT INTO customers (
    customer_id, first_name, last_name, email, phone, date_of_birth,
    signup_date, city, state, zip_code, loyalty_tier, total_spend,
    num_orders, last_order_date, newsletter_opt_in, preferred_contact, notes
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""

INSERT_PIPELINE_RUN = """
INSERT INTO pipeline_runs (
    run_id, run_timestamp, input_file, input_hash, total_records,
    clean_records, rejected_records, warning_records, cleaning_actions
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
"""

INSERT_VALIDATION_ISSUE = """
INSERT INTO validation_issues (
    run_id, customer_id, row_number, field, rule_id, severity,
    message, original_value, corrected_value
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
"""


def _bool_to_int(value: Optional[bool]) -> Optional[int]:
    """Convert Python bool to SQLite integer (0/1/NULL)."""
    if value is None:
        return None
    return 1 if value else 0


def write_database(
    db_path: str,
    customers: list[CustomerRecord],
    pipeline_run: PipelineRun,
    issues: list[ValidationIssue],
) -> None:
    """
    Write all pipeline output to a SQLite database.

    This function is idempotent: it drops and recreates all tables
    before inserting data. All operations run in a single transaction.

    Args:
        db_path: Path to the SQLite database file.
        customers: List of clean CustomerRecord objects to insert.
        pipeline_run: PipelineRun metadata.
        issues: List of all ValidationIssue objects (all severities).
    """
    # Ensure the output directory exists
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(db_path)
    try:
        cursor = conn.cursor()

        # Drop and recreate tables (idempotent)
        cursor.execute(DROP_VALIDATION_ISSUES)
        cursor.execute(DROP_CUSTOMERS)
        cursor.execute(DROP_PIPELINE_RUNS)

        cursor.execute(CREATE_PIPELINE_RUNS)
        cursor.execute(CREATE_CUSTOMERS)
        cursor.execute(CREATE_VALIDATION_ISSUES)

        # Insert pipeline run metadata
        cursor.execute(INSERT_PIPELINE_RUN, (
            pipeline_run.run_id,
            pipeline_run.run_timestamp,
            pipeline_run.input_file,
            pipeline_run.input_hash,
            pipeline_run.total_records,
            pipeline_run.clean_records,
            pipeline_run.rejected_records,
            pipeline_run.warning_records,
            pipeline_run.cleaning_actions,
        ))

        # Insert clean customer records
        for customer in customers:
            cursor.execute(INSERT_CUSTOMER, (
                customer.customer_id,
                customer.first_name,
                customer.last_name,
                customer.email,
                customer.phone,
                customer.date_of_birth,
                customer.signup_date,
                customer.city,
                customer.state,
                customer.zip_code,
                customer.loyalty_tier,
                customer.total_spend,
                customer.num_orders,
                customer.last_order_date,
                _bool_to_int(customer.newsletter_opt_in),
                customer.preferred_contact,
                customer.notes,
            ))

        # Insert all validation issues
        for issue in issues:
            cursor.execute(INSERT_VALIDATION_ISSUE, (
                issue.run_id,
                issue.customer_id,
                issue.row_number,
                issue.field,
                issue.rule_id,
                issue.severity.value if hasattr(issue.severity, 'value') else issue.severity,
                issue.message,
                issue.original_value,
                issue.corrected_value,
            ))

        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
