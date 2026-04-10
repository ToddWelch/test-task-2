"""
CLI entry point for the customer data pipeline.

Usage:
    python pipeline.py data/customers.csv
    python pipeline.py data/customers.csv --output output/customers.db
    python pipeline.py data/customers.csv --dry-run
    python pipeline.py data/customers.csv --verbose
"""

import argparse
import hashlib
import os
import sys
from collections import Counter
from datetime import datetime

from src.loader import load_csv
from src.validators import validate_record
from src.transformer import transform_record
from src.writer import write_database
from src.report import generate_report
from src.models import PipelineRun, Severity


def compute_file_hash(file_path: str) -> str:
    """Compute SHA256 hash of a file."""
    sha256 = hashlib.sha256()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            sha256.update(chunk)
    return sha256.hexdigest()


def generate_run_id() -> str:
    """
    Generate a run ID in the format: YYYYMMDD-HHMMSS-XXXXXX
    where XXXXXX is 6 random hex characters.
    """
    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    random_hex = os.urandom(3).hex()
    return f"{timestamp}-{random_hex}"


def run_pipeline(
    input_file: str,
    output_db: str,
    report_path: str,
    dry_run: bool = False,
    verbose: bool = False,
) -> dict:
    """
    Execute the full pipeline: load, validate, transform, write, report.

    Args:
        input_file: Path to the input CSV file.
        output_db: Path to the output SQLite database.
        report_path: Path to the output validation report JSON.
        dry_run: If True, validate only (no SQLite write).
        verbose: If True, print detailed progress.

    Returns:
        A dict with pipeline results summary.
    """
    run_id = generate_run_id()
    run_timestamp = datetime.now().isoformat()

    if verbose:
        print(f"Run ID: {run_id}")
        print(f"Input:  {input_file}")
        print(f"Output: {output_db}")
        print()

    # Step 1: Compute file hash
    input_hash = compute_file_hash(input_file)
    if verbose:
        print(f"Input hash (SHA256): {input_hash}")

    # Step 2: Load CSV
    if verbose:
        print("Loading CSV...")
    records, row_metadata = load_csv(input_file)
    total_records = len(records)
    if verbose:
        print(f"Loaded {total_records} records")

    # Step 3: Validate and transform each record
    if verbose:
        print("Validating and transforming records...")

    clean_customers = []
    all_issues = []
    seen_emails: set[str] = set()
    seen_customer_ids: set[str] = set()
    fuzzy_dupes: dict[str, list[tuple[str, int]]] = {}
    warning_record_ids: set[int] = set()

    for i, (record, meta) in enumerate(zip(records, row_metadata)):
        cleaned_record, issues, has_errors = validate_record(
            record=record,
            run_id=run_id,
            row_meta=meta,
            seen_emails=seen_emails,
            fuzzy_dupes=fuzzy_dupes,
        )
        all_issues.extend(issues)

        if not has_errors:
            customer = transform_record(cleaned_record, issues)
            if customer is not None:
                # Disambiguate duplicate customer_ids by appending row number
                if customer.customer_id in seen_customer_ids:
                    customer.customer_id = f"{customer.customer_id}-R{record.row_number}"
                seen_customer_ids.add(customer.customer_id)
                clean_customers.append(customer)
                # Track if this clean record has warnings
                has_warnings = any(
                    issue.severity == Severity.WARNING for issue in issues
                )
                if has_warnings:
                    warning_record_ids.add(record.row_number)

        if verbose and (i + 1) % 100 == 0:
            print(f"  Processed {i + 1}/{total_records} records...")

    # Compute stats
    clean_count = len(clean_customers)
    rejected_count = total_records - clean_count
    warning_count = len(warning_record_ids)
    cleaning_actions = sum(
        1 for issue in all_issues if issue.severity == Severity.CLEANED
    )

    # Build PipelineRun metadata
    pipeline_run = PipelineRun(
        run_id=run_id,
        run_timestamp=run_timestamp,
        input_file=input_file,
        input_hash=input_hash,
        total_records=total_records,
        clean_records=clean_count,
        rejected_records=rejected_count,
        warning_records=warning_count,
        cleaning_actions=cleaning_actions,
    )

    # Step 4: Write to SQLite (unless dry run)
    if not dry_run:
        if verbose:
            print(f"Writing {clean_count} records to {output_db}...")
        write_database(output_db, clean_customers, pipeline_run, all_issues)
    else:
        if verbose:
            print("Dry run: skipping database write")

    # Step 5: Generate validation report
    if verbose:
        print(f"Generating report: {report_path}...")
    report_data = generate_report(report_path, pipeline_run, all_issues)

    # Compute top issues for display
    rule_counts = Counter(i.rule_id for i in all_issues)
    top_issues = rule_counts.most_common(5)

    # Print summary to stdout
    print()
    print("Pipeline Complete")
    print("=================")
    print(f"Input:     {input_file} ({total_records} records)")
    if not dry_run:
        print(f"Output:    {output_db}")
    else:
        print(f"Output:    (dry run, no database written)")
    print(f"Run ID:    {run_id}")
    print()
    print("Results:")
    print(f"  Clean records loaded:    {clean_count}")
    print(f"  Rejected (errors):       {rejected_count}")
    print(f"  Warnings on clean:       {warning_count}")
    print(f"  Auto-cleaned fields:     {cleaning_actions}")
    print()
    print("Top issues:")
    for rank, (rule_id, count) in enumerate(top_issues, 1):
        print(f"  {rank}. {rule_id} - {count} occurrences")
    print()
    print(f"Validation report: {report_path}")
    if not dry_run:
        print(f"HTML report:       report/index.html")

    return {
        "run_id": run_id,
        "total_records": total_records,
        "clean_records": clean_count,
        "rejected_records": rejected_count,
        "warning_records": warning_count,
        "cleaning_actions": cleaning_actions,
    }


def main():
    """Parse CLI arguments and run the pipeline."""
    parser = argparse.ArgumentParser(
        description="Customer data validation and cleaning pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Examples:\n"
            "  python pipeline.py data/customers.csv\n"
            "  python pipeline.py data/customers.csv --output output/customers.db\n"
            "  python pipeline.py data/customers.csv --dry-run\n"
            "  python pipeline.py data/customers.csv --verbose\n"
        ),
    )

    parser.add_argument(
        "input_file",
        help="Path to the input CSV file",
    )
    parser.add_argument(
        "--output",
        default="output/customers.db",
        help="Path to the output SQLite database (default: output/customers.db)",
    )
    parser.add_argument(
        "--report",
        default="output/validation_report.json",
        help="Path to the validation report JSON (default: output/validation_report.json)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate only, do not write to SQLite",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Print detailed progress information",
    )

    args = parser.parse_args()

    # Validate input file exists
    if not os.path.exists(args.input_file):
        print(f"Error: Input file not found: {args.input_file}", file=sys.stderr)
        sys.exit(1)

    run_pipeline(
        input_file=args.input_file,
        output_db=args.output,
        report_path=args.report,
        dry_run=args.dry_run,
        verbose=args.verbose,
    )


if __name__ == "__main__":
    main()
