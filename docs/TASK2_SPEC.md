# CLAUDE.md -- Task 2: Data Pipeline & Quality Engineering

## Project Context

This is Task 2 of a paid technical assessment for the Head of Technology role
at Schneider Saddlery ($50M/year equestrian e-commerce). The evaluators are
CEO Eric Schneider and fractional CTO Doug Kerwin (VP Engineering background,
e-commerce at A.C. Moore and Pep Boys). Task 1 (AI-Powered OOS Intelligence
Tool) is complete and lives at github.com/ToddWelch/test-task-1.

Their stack: Shopify (primary storefront), Fulfil.io (ERP), Python/JS mix.

**Time budget:** 3 hours remaining of 8 total (5 spent on Task 1).
**Repo:** github.com/ToddWelch/test-task-2

## What We Are Building

A production-minded data pipeline that ingests a messy 500-row customer CSV,
validates it against 25+ rules, cleans what can be cleaned, rejects what
cannot, and outputs:

1. A SQLite database with clean records (schema-enforced)
2. A standalone HTML validation report (no backend, drag-and-drop CSV upload,
   filterable results table, print/export buttons)
3. A CLI pipeline entry point
4. A README covering production adaptation, verification strategy, and
   time tracking

## Hard Rules (Apply to ALL Output)

1. **No em dashes.** Use commas, periods, semicolons, colons, or parentheses.
   This is non-negotiable across code, comments, docs, and UI copy.
2. **No silent data drops.** Every record must land in either the clean output
   or the validation report. Never substitute defaults for missing data without
   explicitly flagging it.
3. **Idempotent.** Running the pipeline twice on the same input produces
   identical output. Use DROP/recreate for SQLite tables.
4. **Python only.** No TypeScript, no Node. Standard library plus minimal deps.
5. **Ask before guessing.** If a requirement is ambiguous, ask Todd rather
   than assuming.

## Agent Workflow

### Morpheus (Builder)
- Model: claude-sonnet-4-20250514 or current default
- Handles all code creation, file generation, and implementation
- Produces a plan before building; waits for approval
- Returns structured completion summaries

### Crash Override (Reviewer)
- Model: claude-opus-4-20250514 or current max
- Reviews all code before merge to main
- Read-only; cannot modify code directly
- Checks for: correctness, edge cases, security, style consistency,
  em dash violations, silent data drops, idempotency violations

### Task Flow
1. Todd gives a task
2. Morpheus produces a plan
3. Todd approves (or Crash Override reviews the plan first)
4. Morpheus builds on a feature branch
5. Crash Override reviews the build
6. Todd approves merge to main

## Project Structure

```
test-task-2/
  README.md                    # Deliverable README (requirements below)
  CLAUDE.md                    # This file
  requirements.txt             # Python dependencies (minimal)
  pipeline.py                  # CLI entry point
  src/
    __init__.py
    loader.py                  # CSV ingestion and normalization
    validators.py              # All validation rules
    transformer.py             # Data cleaning and transformation
    writer.py                  # SQLite output writer
    models.py                  # Data models (dataclasses or Pydantic)
    report.py                  # Generates validation report data
    constants.py               # Enums, valid states, tier mappings, etc.
  report/
    index.html                 # Standalone HTML validation report
  output/
    customers.db               # SQLite output (generated, gitignored)
    validation_report.json     # Machine-readable report (generated)
  tests/
    test_validators.py         # Unit tests for validation rules
    test_pipeline.py           # Integration tests
    test_idempotency.py        # Proves idempotent behavior
  data/
    customers.csv              # Input file (provided)
```

## Input Data: customers.csv

500 rows, 17 columns. Known issues from analysis:

| Issue | Count | Category |
|-------|-------|----------|
| Leading/trailing whitespace | 50 | Cleanable |
| Missing email | 33 | Validation failure |
| Duplicate emails | 19 | Validation failure |
| Empty phones | 16 | Warn only |
| Missing DOB | 14 | Warn only |
| Invalid state formats ("ca.", "N.Y.", "ohio", "il", "Florida") | 5 | Cleanable |
| Bad zip codes (non-5-digit, "123-45") | 16 | Validation failure |
| Missing last name | 12 | Validation failure |
| Missing first name | 10 | Validation failure |
| Inconsistent phone formats (7 different patterns) | ~76 | Cleanable |
| Case-inconsistent loyalty tiers ("SILVER", "gold") | 11 | Cleanable |
| Mixed boolean newsletter values ("True", "1", "yes", "Y") | ~12 | Cleanable |
| Future birth dates (year 2045) | 8 | Validation failure |
| Negative spend sentinel (-999.99) | 8 | Validation failure |
| Double @@ emails | 5 | Validation failure |
| Non-ISO date formats (MM/DD/YYYY, MM-DD-YYYY, M/DD/YY) | ~20 | Cleanable |
| "N/A" as date_of_birth | 5 | Treat as missing |
| Last order before signup date | 41 | Validation failure |
| Missing signup_date | 5 | Validation failure |
| Empty notes column (all 500) | 500 | Ignore |
| Missing loyalty_tier | 13 | Warn only |
| Missing preferred_contact | 9 | Warn only |

## Validation Rules (25+ Rules)

Organize rules into severity levels:

### ERROR (record goes to reject pile, not loaded into SQLite)
1. Missing email address (required field for customer record)
2. Invalid email format (double @@, missing TLD, consecutive dots, leading/
   trailing dots in local part)
3. Duplicate email address (keep first occurrence, reject subsequent)
4. Missing both first_name AND last_name (need at least one)
5. Future birth date (DOB after today)
6. DOB makes customer under 13 years old (COPPA compliance)
7. DOB makes customer over 120 years old (data error)
8. Negative total_spend (sentinel value, not real data)
9. Invalid zip code format (not 5 digits after cleaning)
10. Missing signup_date (cannot establish customer timeline)
11. Last order date before signup date (temporal impossibility)
12. Signup date in the future
13. Last order date in the future
14. Zero orders but positive spend (logical impossibility)
15. Positive orders but zero or missing spend (logical impossibility)

### WARNING (record loads into SQLite but flagged in report)
16. Missing first_name (but last_name exists)
17. Missing last_name (but first_name exists)
18. Missing phone number
19. Missing DOB
20. Missing loyalty_tier
21. Missing preferred_contact
22. Name contains digits or special characters
23. Single-character name
24. City contains digits
25. Loyalty tier inconsistent with spend (Platinum with <$100,
    Bronze with >$4000)
26. Average order value outlier (total_spend / num_orders > $1000
    or < $1)
27. Phone number missing area code (7 digits only)
28. Fuzzy duplicate detection: same normalized last_name + same zip
    + different email (potential duplicate customer)

### CLEANABLE (auto-corrected, original value logged)
29. Whitespace trimming (all fields)
30. State normalization ("ca." to "CA", "Florida" to "FL",
    "N.Y." to "NY", "ohio" to "OH")
31. Phone normalization (all formats to (XXX) XXX-XXXX)
32. Loyalty tier case normalization ("gold" to "Gold")
33. Newsletter boolean normalization ("yes"/"Y"/"1"/"TRUE" to True)
34. Date format normalization (MM/DD/YYYY and MM-DD-YYYY to
    YYYY-MM-DD ISO 8601)
35. "N/A" values converted to NULL

## SQLite Schema

```sql
CREATE TABLE customers (
    customer_id TEXT PRIMARY KEY,
    first_name TEXT,
    last_name TEXT,
    email TEXT NOT NULL UNIQUE,
    phone TEXT,
    date_of_birth TEXT,          -- ISO 8601 (YYYY-MM-DD) or NULL
    signup_date TEXT NOT NULL,    -- ISO 8601 (YYYY-MM-DD)
    city TEXT,
    state TEXT,                  -- 2-letter uppercase abbreviation
    zip_code TEXT,               -- 5-digit string
    loyalty_tier TEXT,           -- Bronze/Silver/Gold/Platinum or NULL
    total_spend REAL,
    num_orders INTEGER,
    last_order_date TEXT,        -- ISO 8601 (YYYY-MM-DD) or NULL
    newsletter_opt_in INTEGER,   -- 0 or 1
    preferred_contact TEXT,      -- email/phone/mail or NULL
    notes TEXT,
    created_at TEXT DEFAULT (datetime('now')),
    updated_at TEXT DEFAULT (datetime('now'))
);

CREATE TABLE pipeline_runs (
    run_id TEXT PRIMARY KEY,
    run_timestamp TEXT NOT NULL,
    input_file TEXT NOT NULL,
    input_hash TEXT NOT NULL,     -- SHA256 of input file for idempotency
    total_records INTEGER,
    clean_records INTEGER,
    rejected_records INTEGER,
    warning_records INTEGER,
    cleaning_actions INTEGER
);

CREATE TABLE validation_issues (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT NOT NULL,
    customer_id TEXT,
    row_number INTEGER,
    field TEXT,
    rule_id TEXT,
    severity TEXT,               -- ERROR/WARNING/CLEANED
    message TEXT,
    original_value TEXT,
    corrected_value TEXT,         -- Only for CLEANED severity
    FOREIGN KEY (run_id) REFERENCES pipeline_runs(run_id)
);
```

## HTML Validation Report (report/index.html)

Standalone single-file HTML. No backend. No external dependencies except
CDN-loaded libraries (Tailwind CSS via CDN is acceptable; anything else
should be inline).

### Features
- Drag-and-drop CSV upload zone (or file picker button)
- Processes any CSV with matching column names client-side
- Summary dashboard at top: total records, clean count, rejected count,
  warning count, cleaning actions count
- Color-coded severity badges (red for ERROR, amber for WARNING,
  green for CLEANED)
- Filterable/sortable results table showing every issue found
- Click a row to see the full original record
- Print button (clean print stylesheet, no UI chrome)
- Export results to CSV button
- Responsive layout
- Schneider-adjacent styling: clean, professional, data-focused. Do not
  copy sstack.com branding but use a similar "serious e-commerce tool"
  aesthetic. Dark header, white content area, subtle accent color.

### Architecture
- All validation logic must be duplicated in JavaScript to match the
  Python pipeline exactly. This means the same 25+ rules run in-browser.
- Use vanilla JS or minimal framework. No React, no build step.
- The HTML file must work by double-clicking it from a file explorer.

## CLI Interface

```bash
# Basic usage
python pipeline.py data/customers.csv

# With options
python pipeline.py data/customers.csv --output output/customers.db --report output/validation_report.json

# Dry run (validate only, no SQLite write)
python pipeline.py data/customers.csv --dry-run

# Verbose mode
python pipeline.py data/customers.csv --verbose
```

Output should include a summary table printed to stdout:

```
Pipeline Complete
=================
Input:     data/customers.csv (500 records)
Output:    output/customers.db
Run ID:    20260410-143022-a1b2c3

Results:
  Clean records loaded:    XXX
  Rejected (errors):       XX
  Warnings on clean:       XX
  Auto-cleaned fields:     XX

Top issues:
  1. [rule_name] - XX occurrences
  2. [rule_name] - XX occurrences
  3. [rule_name] - XX occurrences

Validation report: output/validation_report.json
HTML report:       report/index.html
```

## Dependencies (requirements.txt)

Keep this minimal to show production discipline:

```
pydantic>=2.0      # Data validation and models
```

That is it. CSV, SQLite, argparse, datetime, hashlib, json, re, pathlib are
all standard library. Do not add pandas, numpy, or anything heavy. This is a
pipeline, not a notebook.

## README.md Requirements

The README is a deliverable. It must cover:

### 1. Overview
What this pipeline does in 2-3 sentences.

### 2. Quick Start
How to install deps, run the pipeline, and view the report. Copy-paste ready.

### 3. Architecture
Brief explanation of the modular design and data flow:
CSV -> Loader -> Validator -> Transformer -> Writer -> SQLite

### 4. Validation Rules
Table or list of all rules with severity levels.

### 5. Production Adaptation
How this pipeline would adapt for a real API target (Shopify Customer API,
Fulfil.io ERP sync) with rate limits. Cover:
- Rate limiting strategy (token bucket, exponential backoff with jitter)
- Batch processing (Shopify allows 250 records per bulk operation)
- Checkpoint/resume for large datasets (write progress to pipeline_runs
  table so a crashed run can restart from where it left off)
- API response validation (do not trust 200 OK; verify the record
  actually persisted by reading it back on a sample basis)
- Idempotency keys for API calls (use customer_id as natural key,
  check if exists before create vs update)
- Webhook integration for real-time data quality monitoring

Reference Todd's real-world experience:
- SP-API integration for Amazon (pull/push listings by SKU with
  rate limit handling)
- Sellerboard CSV parsing (non-breaking space thousand separators,
  production data normalization)
- TutorBird 62-column CSV import pipeline (complex multi-format
  ingestion with validation)
- RestockPro/AnalyzerTools/ListingMirror multi-source data
  reconciliation

### 6. Pre-Production Checks
What checks to run before executing against production:
- Schema validation against target API documentation
- Dry run against staging/sandbox environment
- Row count reconciliation (input vs output vs rejected)
- Spot check sample of cleaned records against originals
- Duplicate detection across existing production data (not just
  within the import batch)
- Rollback plan (tag current state, import in transaction,
  verify before committing)

### 7. Output Verification
How to verify the output data is correct:
- SHA256 hash comparison for idempotency proof
- SQL queries to validate business rules post-load
- Cross-reference rejection counts with known issue counts
- Statistical profile comparison (mean spend, tier distribution)
  between input and output to catch systemic data loss

### 8. AI Tools Used
Brief note: Built with Claude Code using Morpheus (builder) and
Crash Override (reviewer) agent workflow. Independent code review
by OpenAI Codex. Spec document created collaboratively in Claude.ai.

### 9. Time Tracking
Hours spent on Task 2, broken down by phase.

## Testing Strategy

### Unit Tests (test_validators.py)
- Test each validation rule independently with known-good and
  known-bad inputs
- Edge cases: empty strings, None values, boundary dates,
  Unicode characters

### Integration Tests (test_pipeline.py)
- Run full pipeline on a small synthetic CSV (10-20 rows) with
  one example of each issue type
- Verify correct records land in SQLite
- Verify rejected records appear in validation report
- Verify cleaning actions are logged

### Idempotency Tests (test_idempotency.py)
- Run pipeline twice on same input
- Compare SQLite contents (should be identical)
- Compare validation report (should be identical)
- Compare pipeline_runs table (should show same hash,
  different run_id and timestamp)

## Git Workflow

1. Initialize repo with this CLAUDE.md, README.md skeleton, and .gitignore
2. Work on feature branches (feature/pipeline-core, feature/html-report, etc.)
3. Crash Override reviews before merge to main
4. Commit messages: clear, present tense, no em dashes
5. .gitignore: output/*.db, __pycache__/, *.pyc, .env

## Completion Protocol

When Morpheus finishes a task:

```
MORPHEUS BUILD COMPLETE

What was built:
[description]

Files created/modified:
[list]

Commits:
[list with messages]

Ready for review:
[yes/no, and what specifically should be reviewed]

Open questions:
[anything unresolved]

Next steps:
[what comes after this task]
```

## Build Order

Execute in this sequence:

### Phase 1: Foundation
- [ ] Initialize repo, .gitignore, requirements.txt
- [ ] Create src/constants.py (valid states, state mappings, tier values,
      disposable domains list)
- [ ] Create src/models.py (Pydantic models for CustomerRecord,
      ValidationIssue, PipelineRun)

### Phase 2: Pipeline Core
- [ ] Create src/loader.py (CSV reader with encoding detection,
      whitespace stripping, date normalization)
- [ ] Create src/validators.py (all 28+ validation rules organized
      by severity)
- [ ] Create src/transformer.py (cleaning/normalization logic)
- [ ] Create src/writer.py (SQLite schema creation, record insertion,
      run logging)
- [ ] Create src/report.py (JSON report generation)
- [ ] Create pipeline.py (CLI with argparse)

### Phase 3: Testing
- [ ] Create tests/test_validators.py
- [ ] Create tests/test_pipeline.py
- [ ] Create tests/test_idempotency.py
- [ ] Run full pipeline on customers.csv, verify output

### Phase 4: HTML Report
- [ ] Create report/index.html (standalone, all validation logic
      in JS matching Python rules)
- [ ] Test with drag-drop of customers.csv
- [ ] Verify print stylesheet
- [ ] Verify CSV export

### Phase 5: Documentation
- [ ] Complete README.md with all required sections
- [ ] Final Crash Override review of entire repo
- [ ] Clean up, tag release

## What Impresses the Evaluators

Doug Kerwin (VP Engineering, e-commerce) will look for:
- Clean separation of concerns (not one monolithic script)
- Production patterns (logging, error handling, idempotency)
- Realistic understanding of e-commerce data problems
- Evidence of testing discipline

Eric Schneider (CEO) will look for:
- Clear documentation a non-engineer can follow
- The HTML report as a tangible, visual deliverable
- Practical thinking about real-world adaptation
- Time management (delivering quality within budget)

Do not over-engineer. Do not add features not in this spec. Focus on
clean execution of what is here.
