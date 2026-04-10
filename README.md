# Customer Data Pipeline: Quality Engineering Assessment

## Overview

A production-grade data pipeline that ingests a messy 500-row customer CSV, validates it against 35 rules across three severity levels (ERROR, WARNING, CLEANED), and outputs a clean SQLite database alongside a detailed validation report. Built as Task 2 of the Schneider Saddlery Head of Technology technical assessment, this pipeline demonstrates the kind of data quality engineering required when syncing customer records between Shopify, Fulfil.io, and third-party data sources.

The pipeline processed 500 input records, loaded 367 clean records into SQLite, rejected 133 records with errors, flagged 79 records with warnings, and auto-cleaned 175 field values.

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
| Auto-cleaned field values | 175 |

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

## AI Tools Used

Built with Claude Code using Morpheus (builder) and Crash Override (reviewer) agent workflow. Independent code review by OpenAI Codex. Spec document created collaboratively in Claude.ai.

## Time Tracking

| Phase | Time |
|-------|------|
| Planning and spec | ~30 min |
| Phase 1: Foundation (models, constants, project setup) | ~15 min |
| Phase 2: Pipeline Core (loader, validators, transformer, writer, CLI) | ~45 min |
| Phase 3: Testing (unit, integration, idempotency tests) | ~30 min |
| Phase 4: HTML Report (standalone validation viewer) | ~30 min |
| Phase 5: Documentation (this README) | ~20 min |
| Review and polish | ~10 min |
| **Total** | **~3 hours** |
