# Customer Data Pipeline: Quality Engineering Assessment

## Overview

A production-grade data pipeline that ingests a messy 500-row customer CSV, validates it against 35 rules across three severity levels (ERROR, WARNING, CLEANED), and outputs a clean SQLite database alongside a detailed validation report. Built as Task 2 of the Schneider Saddlery Head of Technology technical assessment, this pipeline demonstrates the kind of data quality engineering required when syncing customer records between Shopify, Fulfil.io, and third-party data sources.

The pipeline processed 500 input records, loaded 367 clean records into SQLite, rejected 133 records with errors, flagged 79 records with warnings, and auto-cleaned 841 field values.

---

## Quick Start

### Prerequisites

- Python 3.10 or higher
- pip (Python package manager)

### Install and Run

```bash
# Clone the repository
git clone https://github.com/ToddWelch/test-task-2.git
cd test-task-2

# Install dependencies (just Pydantic, everything else is standard library)
pip install -r requirements.txt

# Run the pipeline
python pipeline.py data/customers.csv

# Run with verbose output
python pipeline.py data/customers.csv --verbose

# Dry run (validate only, no database write)
python pipeline.py data/customers.csv --dry-run

# Custom output location
python pipeline.py data/customers.csv --output output/customers.db --report output/validation_report.json
```

### View the Report

Open `report/index.html` in any browser (just double-click the file). You can also drag and drop any CSV with matching column names to validate it client-side.

### Run Tests

```bash
python -m unittest discover tests/ -v
```

---

## Architecture

The pipeline follows a modular, single-responsibility design. Each stage is a separate Python module that can be tested and modified independently.

```
CSV File
  |
  v
[Loader] --> Raw records (list of dicts)
  |
  v
[Validator] --> Issues identified (ERROR / WARNING)
  |
  v
[Transformer] --> Cleaned records (CLEANED actions logged)
  |
  v
[Writer] --> SQLite database + pipeline run metadata
  |
  v
[Report] --> JSON validation report
```

### Source Modules

| Module | File | Responsibility |
|--------|------|----------------|
| **Loader** | `src/loader.py` | Reads CSV with encoding detection, strips whitespace, normalizes empty values. Produces raw record dicts. |
| **Validators** | `src/validators.py` | Applies all 35 validation rules organized by severity. Returns a list of ValidationIssue objects per record. |
| **Transformer** | `src/transformer.py` | Performs auto-cleaning: state normalization, phone formatting, date standardization, tier casing, boolean normalization, and sentinel removal. Logs every change. |
| **Writer** | `src/writer.py` | Creates SQLite schema (DROP/recreate for idempotency), inserts clean records, logs pipeline run metadata and all validation issues. |
| **Report** | `src/report.py` | Generates the JSON validation report consumed by the HTML viewer. |
| **Models** | `src/models.py` | Pydantic data models for CustomerRecord, ValidationIssue, and PipelineRun. Enforces type safety throughout the pipeline. |
| **Constants** | `src/constants.py` | Enums, valid state codes, state name-to-abbreviation mappings, loyalty tier values, and other reference data. |
| **Pipeline CLI** | `pipeline.py` | Argparse-based entry point. Orchestrates the full flow and prints summary statistics. |

---

### Three Severity Tiers: Why Separate Errors from Warnings from Cleaning

A simpler pipeline would have two buckets: pass or fail. Three tiers exist because that is how production data operations actually work:

- **ERROR** means the record is fundamentally broken. Missing email, negative spend sentinel, temporal impossibility. These cannot be loaded without risking downstream corruption. A customer with no email cannot receive order confirmations, abandoned cart flows, or shipping notifications. Loading it silently creates a time bomb.

- **WARNING** means the record is loadable but suspicious. A Bronze tier customer with $4,900 in lifetime spend probably got mis-tiered during a migration. The record works, but someone should investigate. Loading it with a flag lets operations continue while surfacing the anomaly.

- **CLEANED** means the pipeline fixed something automatically and logged what it changed. Phone formatting, state abbreviation normalization, date format conversion. These are safe to auto-correct because the intent is unambiguous: "ca." is California, "1" for newsletter means True. But the original value is always preserved in the audit trail so corrections can be reviewed or reverted.

This mirrors how platforms like Fulfil.io handle data imports: records are accepted, accepted with warnings, or rejected, with a downloadable report of everything that happened.

### Why a Standalone HTML Report

The brief asked for a validation report. A JSON file satisfies the requirement. But the people who actually use data quality reports are operations managers and merchandisers, not engineers. They need to filter by severity, search for specific customers, print a summary for a meeting, or export issues to a spreadsheet for assignment.

The standalone HTML report (no server, no install, double-click to open) serves this audience. It also demonstrates that the validation logic is portable: the same 35 rules run in both Python (for pipeline automation) and JavaScript (for ad-hoc analysis by non-technical users). If a new CSV arrives and someone wants a quick quality check before running the full pipeline, they drop it on the HTML page and get results in seconds.

---

## Validation Rules

All 35 rules organized by severity level:

### ERROR Rules (record rejected, not loaded into SQLite)

| # | Rule ID | Description |
|---|---------|-------------|
| 1 | R01_MISSING_EMAIL | Email address is missing (required field for customer identification) |
| 2 | R02_INVALID_EMAIL | Email format is invalid (double @@, missing TLD, consecutive dots, leading/trailing dots) |
| 3 | R03_DUPLICATE_EMAIL | Duplicate email address detected (first occurrence kept, subsequent rejected) |
| 4 | R04_MISSING_BOTH_NAMES | Both first_name and last_name are missing (at least one required) |
| 5 | R05_FUTURE_DOB | Date of birth is in the future |
| 6 | R06_UNDER_13 | Date of birth indicates customer is under 13 (COPPA compliance) |
| 7 | R07_OVER_120 | Date of birth indicates customer is over 120 years old (data error) |
| 8 | R08_NEGATIVE_SPEND | Total spend is negative (sentinel value, not real data) |
| 9 | R09_INVALID_ZIP | Zip code is not 5 digits after cleaning |
| 10 | R10_MISSING_SIGNUP_DATE | Signup date is missing (cannot establish customer timeline) |
| 11 | R11_ORDER_BEFORE_SIGNUP | Last order date is before signup date (temporal impossibility) |
| 12 | R12_FUTURE_SIGNUP | Signup date is in the future |
| 13 | R13_FUTURE_ORDER | Last order date is in the future |
| 14 | R14_ZERO_ORDERS_POS_SPEND | Zero orders but positive spend (logical impossibility) |
| 15 | R15_POSITIVE_ORDERS_NO_SPEND | Positive order count but zero or missing spend (logical impossibility) |

### WARNING Rules (record loaded into SQLite, flagged in report)

| # | Rule ID | Description |
|---|---------|-------------|
| 16 | R16_MISSING_FIRST_NAME | First name missing (last name exists) |
| 17 | R17_MISSING_LAST_NAME | Last name missing (first name exists) |
| 18 | R18_MISSING_PHONE | Phone number is missing |
| 19 | R19_MISSING_DOB | Date of birth is missing |
| 20 | R20_MISSING_LOYALTY_TIER | Loyalty tier is missing |
| 21 | R21_MISSING_PREFERRED_CONTACT | Preferred contact method is missing |
| 22 | R22_NAME_SPECIAL_CHARS | Name contains digits or special characters |
| 23 | R23_SINGLE_CHAR_NAME | Name is a single character |
| 24 | R24_CITY_HAS_DIGITS | City name contains digits |
| 25 | R25_TIER_SPEND_MISMATCH | Loyalty tier is inconsistent with spend (e.g., Platinum with under $100, Bronze with over $4,000) |
| 26 | R26_AOV_OUTLIER | Average order value outlier (total_spend / num_orders exceeds $1,000 or falls below $1) |
| 27 | R27_PHONE_MISSING_AREA_CODE | Phone number has only 7 digits (missing area code) |
| 28 | R28_FUZZY_DUPLICATE | Potential duplicate: same normalized last_name and zip code but different email |

### CLEANED Rules (auto-corrected, original value logged)

| # | Rule ID | Description |
|---|---------|-------------|
| 29 | R29_WHITESPACE_TRIM | Leading/trailing whitespace removed from all fields |
| 30 | R30_STATE_NORMALIZE | State normalized to 2-letter uppercase abbreviation (e.g., "Florida" to "FL", "ca." to "CA") |
| 31 | R31_PHONE_NORMALIZE | Phone reformatted to (XXX) XXX-XXXX standard format |
| 32 | R32_TIER_NORMALIZE | Loyalty tier case normalized (e.g., "gold" to "Gold", "SILVER" to "Silver") |
| 33 | R33_NEWSLETTER_NORMALIZE | Newsletter opt-in normalized to boolean (e.g., "yes"/"Y"/"1"/"TRUE" to True) |
| 34 | R34_DATE_NORMALIZE | Date format converted to ISO 8601 YYYY-MM-DD (from MM/DD/YYYY, MM-DD-YYYY, M/DD/YY) |
| 35 | R35_NULL_SENTINEL | "N/A" sentinel values converted to NULL |

### Pipeline Run Results (from `data/customers.csv`)

| Metric | Count |
|--------|-------|
| Total input records | 500 |
| Clean records loaded to SQLite | 367 |
| Rejected records (ERROR) | 133 |
| Records with warnings | 79 |
| Auto-cleaned field values | 841 |

---

## HTML Validation Report

The standalone HTML report (`report/index.html`) duplicates all 35 validation rules in JavaScript, enabling client-side processing of any CSV with matching column names. No data leaves the user's machine.

### Upload and Processing

Drag and drop a CSV onto the upload zone (or use the file picker). PapaParse handles CSV parsing client-side. All 35 rules execute in JavaScript with the same logic, thresholds, and edge case handling as the Python pipeline. The report works by double-clicking the file from a file explorer. No server, no build step, no installation.

### Dashboard and Filtering

The summary strip shows five KPI cards: Total Records, Clean, Rejected, Warnings, and Cleaning Actions. Below it, severity filter checkboxes (ERROR in red, WARNING in amber, CLEANED in green) control which issues appear in the table. ERROR and WARNING are checked by default; CLEANED is unchecked to reduce noise on first load.

Additional filters include a Rule ID dropdown, a Field dropdown, and free-text search across rule, field, and message columns. Pagination updates to reflect the active filter state.

### CLEANED Entry Collapsing

CLEANED actions for the same record collapse into a single green summary row showing all rules applied and fields affected (e.g., "R29, R31, R33 | 10 fields | Trimmed whitespace, Normalized phone..."). Clicking the row expands a sub-table with individual cleaning actions. Collapsing is display-only; the CSV export outputs individual uncollapsed rows for machine consumption.

### Print Output

The print button expands all filtered rows (not just the visible page), hides UI controls, and renders a clean document. The table header repeats on every page. The ROW column uses nowrap to prevent number splitting. The RULE and FIELD columns use 7pt nowrap to keep identifiers like R25_TIER_SPEND_MISMATCH intact. A compact KPI strip ensures the data table starts on page 1. Severity badges use ink-friendly styling (white background with colored borders). A tip at the top suggests disabling browser headers and footers for cleaner output.

### Export

The Export CSV button outputs all filtered issues as individual (uncollapsed) rows with columns: row, severity, rule, field, message, original, corrected.

### Dependencies

Tailwind CSS (CDN) for styling and PapaParse (CDN) for CSV parsing. No framework, no build step, no backend, no npm.

---

## Production Adaptation

This pipeline is built as a local CSV-to-SQLite proof of concept. In production at Schneider Saddlery, the target would be the Shopify Customer API or Fulfil.io ERP, not a local database. Here is how the architecture would adapt.

### Rate Limiting Strategy

Production APIs enforce rate limits. Shopify's REST Admin API allows 40 requests per app per store, refilling at 2 per second. Fulfil.io has similar constraints.

**Token bucket algorithm:** Maintain a counter of available tokens that refills at the API's documented rate. Before each request, check if a token is available. If not, wait until one refills. This prevents burst-then-throttle patterns that waste time on 429 retries.

**Exponential backoff with jitter:** When a 429 (rate limited) or 5xx response is received, retry with exponential delay: 1s, 2s, 4s, 8s, up to a maximum of 60s. Add random jitter (0 to 50% of the delay) to prevent synchronized retry storms when multiple workers hit the limit simultaneously.

This is the same pattern used in Todd's SP-API integration for Amazon, where pull/push operations on listings by SKU required careful rate limit management across multiple API endpoints with different throttle rates.

### Batch Processing

Shopify's bulk operations API allows up to 250 records per mutation. Rather than sending 500 individual API calls, the pipeline would:

1. Chunk clean records into batches of 250
2. Submit each batch as a single GraphQL bulk mutation
3. Poll for completion (bulk operations are asynchronous)
4. Parse the JSONL result file for per-record success/failure status

This reduces 500 API calls to 2 batch operations, dramatically improving throughput while staying within rate limits.

### Checkpoint and Resume

For large datasets (10,000+ records), the pipeline needs crash recovery. The existing `pipeline_runs` table already tracks run metadata. In production, this would extend to:

- Write a checkpoint after each successful batch (batch number, last processed record ID, timestamp)
- On startup, check for incomplete runs and resume from the last checkpoint
- Use the input file's SHA256 hash to detect if the source data changed between resume attempts
- If the source changed, force a full restart rather than resuming with stale checkpoint data

This pattern mirrors Todd's TutorBird 62-column CSV import pipeline, where large multi-format ingestion runs needed to survive interruptions without re-processing already-imported records.

### API Response Validation

Do not trust HTTP 200 as proof that a record persisted. Production validation includes:

- **Read-back verification:** After creating a customer via API, read it back on a sample basis (e.g., every 10th record) to confirm the data actually stored correctly
- **Field-level comparison:** Compare the API response fields against what was sent, flagging any silent transformations the API applied
- **Eventual consistency handling:** For APIs with async processing, poll until the record is confirmed or a timeout is reached

### Idempotency Keys

Use `customer_id` as the natural idempotency key. Before creating a new customer:

1. Query the target system by email (the unique business identifier)
2. If the customer exists, issue an UPDATE instead of CREATE
3. Include an `Idempotency-Key` header (for APIs that support it) using a hash of the customer_id plus a run identifier
4. Log every create vs. update decision for audit purposes

This prevents duplicate customer records when a pipeline run is retried after a partial failure.

### Webhook Integration for Real-Time Monitoring

Instead of running the pipeline on a schedule, production systems can use webhooks for real-time data quality monitoring:

- Register Shopify webhooks for `customers/create` and `customers/update`
- Run validation rules against incoming webhook payloads in real time
- Flag data quality issues immediately (e.g., a new customer created with an invalid email by a store associate)
- Aggregate quality metrics into a dashboard for operations teams

### Real-World Context

This pipeline's architecture draws from production experience with multi-source e-commerce data:

- **SP-API for Amazon:** Pull/push listings by SKU with per-endpoint rate limit handling, token refresh, and retry logic across throttled API calls
- **Sellerboard CSV parsing:** Production data normalization including non-breaking space thousand separators (U+00A0) that silently break numeric parsing if not detected
- **TutorBird 62-column CSV import:** Complex multi-format ingestion with validation across date formats, currency formats, and optional fields that could appear in any order
- **RestockPro, AnalyzerTools, ListingMirror reconciliation:** Multi-source data pipelines where the same product appears in 3-4 systems with different field names, formats, and update cadences, requiring fuzzy matching and conflict resolution

---

## Pre-Production Checks

Before executing this pipeline against a production system, run these checks:

1. **Schema validation against target API documentation.** Confirm every field in the output schema maps to a valid API field. APIs change; field names and types drift between versions.

2. **Dry run against staging/sandbox.** Run `python pipeline.py data/customers.csv --dry-run` to validate without writing. Then run against a Shopify development store or Fulfil.io sandbox to verify API interactions.

3. **Row count reconciliation.** Verify that input records (500) equals clean records (367) plus rejected records (133). No records should be silently dropped. The pipeline enforces this: every record lands in either the clean output or the rejection report.

4. **Spot check sample of cleaned records.** Pull 10-20 cleaned records from SQLite and manually compare against the original CSV. Verify that state normalization, phone formatting, and date conversion produced correct results.

5. **Duplicate detection across existing production data.** The pipeline catches duplicates within the import batch, but production runs must also check against customers already in the target system. Query by email before insert.

6. **Rollback plan.** Before importing:
   - Tag the current state of the target system (Shopify export, database snapshot)
   - Import within a transaction where possible
   - Verify record counts and data integrity before committing
   - If verification fails, roll back to the tagged state

---

## Output Verification

### SHA256 Hash Comparison (Idempotency Proof)

The pipeline records the SHA256 hash of the input file with every run. Running the pipeline twice on the same input produces identical output:

```bash
# First run
python pipeline.py data/customers.csv
# Input hash: e4606bf2155b3a154fe47148cf9ab1c793c31230d0827e244f9b44960cf0a253

# Second run
python pipeline.py data/customers.csv
# Input hash: e4606bf2155b3a154fe47148cf9ab1c793c31230d0827e244f9b44960cf0a253

# Verify identical output
python -c "
import hashlib, sqlite3
conn = sqlite3.connect('output/customers.db')
c = conn.cursor()
c.execute('SELECT * FROM customers ORDER BY customer_id')
print(hashlib.sha256(str(c.fetchall()).encode()).hexdigest())
conn.close()
"
# Output: fc5a837b6022ed92c346df4d5789b2ed4fc310a336cd54564fe0ef5f095a60ca
```

Both runs produce the same customer data hash because the pipeline uses DROP/recreate for the SQLite tables, ensuring no state leakage between runs.

### SQL Queries for Post-Load Validation

```sql
-- Verify no NULL emails (required field)
SELECT COUNT(*) FROM customers WHERE email IS NULL;
-- Expected: 0

-- Verify no duplicate emails
SELECT email, COUNT(*) FROM customers GROUP BY email HAVING COUNT(*) > 1;
-- Expected: 0 rows

-- Verify all states are 2-letter uppercase
SELECT DISTINCT state FROM customers WHERE state IS NOT NULL AND (LENGTH(state) != 2 OR state != UPPER(state));
-- Expected: 0 rows

-- Verify all zip codes are 5 digits
SELECT zip_code FROM customers WHERE zip_code IS NOT NULL AND zip_code NOT GLOB '[0-9][0-9][0-9][0-9][0-9]';
-- Expected: 0 rows

-- Verify no negative spend
SELECT COUNT(*) FROM customers WHERE total_spend < 0;
-- Expected: 0

-- Verify no future dates
SELECT COUNT(*) FROM customers WHERE signup_date > date('now') OR last_order_date > date('now') OR date_of_birth > date('now');
-- Expected: 0

-- Verify order timeline consistency
SELECT COUNT(*) FROM customers WHERE last_order_date < signup_date;
-- Expected: 0
```

### Cross-Reference Rejection Counts

Compare pipeline output against known issue counts from data profiling:

| Known Issue | Expected | Actual |
|-------------|----------|--------|
| Missing email | ~33 | 33 rejected (R01) |
| Invalid email format | ~5 | 26 rejected (R02, includes additional format violations) |
| Duplicate email | ~19 | 17 rejected (R03, after earlier rejections reduce the pool) |
| Last order before signup | ~41 | 41 rejected (R11) |
| Negative spend sentinel | ~8 | 8 rejected (R08) |
| Future birth dates | ~8 | 8 rejected (R05) |

### Statistical Profile Comparison

```sql
-- Compare tier distribution
SELECT loyalty_tier, COUNT(*) as count, ROUND(AVG(total_spend), 2) as avg_spend
FROM customers GROUP BY loyalty_tier ORDER BY avg_spend DESC;

-- Verify mean spend is reasonable (no systemic data loss)
SELECT ROUND(AVG(total_spend), 2) as mean_spend,
       ROUND(MIN(total_spend), 2) as min_spend,
       ROUND(MAX(total_spend), 2) as max_spend
FROM customers;

-- Check for suspicious gaps in customer_id sequence
SELECT COUNT(DISTINCT customer_id) as unique_ids FROM customers;
-- Should equal 367 (clean record count)
```

---

## AI Tools Used

### Claude (claude.ai, Specification and Strategy)

Used as a strategic planning partner before any code was written. The project specification (TASK2_SPEC.md) was developed collaboratively in Claude.ai, including: analyzing the CSV to inventory all data quality issues across 500 rows and 17 columns, designing the 35 validation rules with severity tiers and edge case handling, defining the SQLite schema with audit trail tables, planning the modular project structure, and writing detailed acceptance criteria for the HTML report.

Claude.ai also produced iterative spec patches (TASK2_SPEC_PATCH_001 through PATCH_003) to refine the HTML report based on testing. These addressed: collapsing CLEANED entries per record to reduce noise, adding severity filter checkboxes with CLEANED unchecked by default, fixing the print stylesheet to render all filtered rows across correct page breaks, and preventing column wrapping on rule IDs and row numbers.

### Claude Code (Morpheus/Crash Override Agent Workflow)

All code was built using Claude Code with a two-agent workflow orchestrated via CLAUDE.md conventions:

- **Morpheus** (builder agent): Received implementation tasks with full context from the spec. Produced plans before building, then implemented on feature branches. Built all Python pipeline modules, the test suite, and the 1,900-line standalone HTML report with client-side validation logic.

- **Crash Override** (reviewer agent): Reviewed all code before merge to main. Evaluated correctness, edge cases, security, style consistency, and adherence to project conventions (including the no em dash rule). Read-only; never modified code directly.

The orchestrator (Claude Code) relayed tasks between agents following a strict flow: task, plan, review plan, build, review build, merge. This is the same workflow used across production projects including the Welch Command Center and TTC Greer CRM.

### OpenAI Codex (Independent Code Review)

After the build was complete, the entire repository was submitted to OpenAI Codex for an independent third-party code review. Codex received a detailed review prompt covering all 35 validation rules, edge case specifications, idempotency requirements, security concerns (SQL injection, XSS in the HTML report), and the em dash style constraint. Codex evaluated correctness, security, code quality, testing coverage, and production readiness. Findings were triaged and fixed before submission.

### What AI Did Not Do

AI did not make architectural decisions without human review. Every plan was approved before implementation. Strategy decisions (SQLite over JSON, standalone HTML report with client-side validation, three severity tiers, COPPA age checking) were made by Todd based on production experience with e-commerce data pipelines. All iterative refinement of the HTML report (print stylesheet fixes, severity checkboxes, collapsed CLEANED entries) was driven by Todd testing the actual printed output and directing specific changes through spec patches.

---

## Limitations

- Processes local CSV files only; not connected to a live Shopify or Fulfil.io instance
- Duplicate email detection is within-batch only; does not check against existing customers in a target system
- State normalization covers the 5 variants found in the test data ("ca.", "N.Y.", "ohio", "il", "Florida"); a production system would use a comprehensive state name/abbreviation lookup
- Zip code validation checks format (5 digits) only; does not verify the zip exists or matches the stated city/state (no USPS API call)
- Fuzzy duplicate detection (R28) uses exact last_name + zip match; a production system would use edit distance or phonetic matching
- The HTML report loads Tailwind CSS and PapaParse from CDN, requiring internet access for styling and CSV parsing
- Phone normalization assumes US phone numbers (10-digit with area code)
- No multi-threaded or async processing; performance on datasets over 50,000 rows has not been tested

---

## Roadmap

1. **Live API integration:** Connect to Shopify Customer API and Fulfil.io REST API with rate limit handling, batch operations, and checkpoint/resume for large imports
2. **Cross-system duplicate detection:** Before creating a customer, query the target system by email to determine create vs. update
3. **Configurable rule engine:** Move rule definitions to a YAML or JSON config file so thresholds (AOV outlier, tier/spend mismatch boundaries, COPPA age) can be adjusted without code changes
4. **Historical quality tracking:** Store validation results over time to measure whether data quality improves across import batches
5. **Webhook-triggered validation:** Register for Shopify customers/create webhooks to validate new records in real time
6. **Enhanced fuzzy matching:** Replace exact last_name + zip matching with Jaro-Winkler or Soundex for better duplicate detection
7. **Multi-format support:** Accept JSON, XML, and direct API responses in addition to CSV
8. **International address support:** Extend state/zip validation beyond US formats for Canadian and international customers

---

## Time Spent

| Phase | Time |
|---|---|
| Specification, data analysis, and strategy (Claude.ai) | 0.75 hours |
| Foundation: models, constants, project setup | 0.25 hours |
| Pipeline core: loader, validators, transformer, writer, CLI | 0.75 hours |
| Testing: unit, integration, idempotency | 0.25 hours |
| HTML report: initial build and iterative refinement | 0.75 hours |
| Documentation and README | 0.25 hours |
| Codex independent review and fixes | 0.25 hours |
| **Total** | **3.25 hours** |

Task 1 (AI-Powered OOS Intelligence Tool): 5.0 hours
**Combined assessment total: 8.25 hours**
