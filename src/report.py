"""
Report generator for the customer data pipeline.

Generates a validation_report.json file containing pipeline run metadata,
all validation issues, and summary statistics including top issues by
rule_id count.
"""

import json
from collections import Counter
from pathlib import Path

from src.models import PipelineRun, ValidationIssue, Severity


def generate_report(
    report_path: str,
    pipeline_run: PipelineRun,
    issues: list[ValidationIssue],
) -> dict:
    """
    Generate the validation report as a JSON file.

    Structure:
    {
        "pipeline_run": { ... },
        "summary": {
            "total_records": N,
            "clean_records": N,
            "rejected_records": N,
            "warning_records": N,
            "cleaning_actions": N,
            "total_issues": N,
            "error_count": N,
            "warning_count": N,
            "cleaned_count": N,
            "top_issues": [ { "rule_id": "...", "count": N }, ... ]
        },
        "issues": [ ... ]
    }

    Args:
        report_path: Path to write the JSON report.
        pipeline_run: PipelineRun metadata.
        issues: All validation issues.

    Returns:
        The report dict (also written to disk).
    """
    # Count issues by severity
    error_count = sum(1 for i in issues if i.severity == Severity.ERROR)
    warning_count = sum(1 for i in issues if i.severity == Severity.WARNING)
    cleaned_count = sum(1 for i in issues if i.severity == Severity.CLEANED)

    # Count top issues by rule_id
    rule_counts = Counter(i.rule_id for i in issues)
    top_issues = [
        {"rule_id": rule_id, "count": count}
        for rule_id, count in rule_counts.most_common(10)
    ]

    # Build the report structure
    report = {
        "pipeline_run": {
            "run_id": pipeline_run.run_id,
            "run_timestamp": pipeline_run.run_timestamp,
            "input_file": pipeline_run.input_file,
            "input_hash": pipeline_run.input_hash,
            "total_records": pipeline_run.total_records,
            "clean_records": pipeline_run.clean_records,
            "rejected_records": pipeline_run.rejected_records,
            "warning_records": pipeline_run.warning_records,
            "cleaning_actions": pipeline_run.cleaning_actions,
        },
        "summary": {
            "total_records": pipeline_run.total_records,
            "clean_records": pipeline_run.clean_records,
            "rejected_records": pipeline_run.rejected_records,
            "warning_records": pipeline_run.warning_records,
            "cleaning_actions": pipeline_run.cleaning_actions,
            "total_issues": len(issues),
            "error_count": error_count,
            "warning_count": warning_count,
            "cleaned_count": cleaned_count,
            "top_issues": top_issues,
        },
        "issues": [
            {
                "run_id": issue.run_id,
                "customer_id": issue.customer_id,
                "row_number": issue.row_number,
                "field": issue.field,
                "rule_id": issue.rule_id,
                "severity": issue.severity.value if hasattr(issue.severity, 'value') else issue.severity,
                "message": issue.message,
                "original_value": issue.original_value,
                "corrected_value": issue.corrected_value,
            }
            for issue in issues
        ],
    }

    # Write to disk
    Path(report_path).parent.mkdir(parents=True, exist_ok=True)
    with open(report_path, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, ensure_ascii=False)

    return report
