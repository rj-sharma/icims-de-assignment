# Source Data Analysis

Captures the first-pass source data profiling for the assignment datasets. As a data engineer, I would normally start here before designing ingestion, transformations, tests, and the target model. Focusing on below items:
- File formats and expected schemas
- Primary/business keys
- Nulls and duplicates
- Mixed date formats
- Relationship integrity across files
- Data cleanup required before analytics

## Source Files

| File | Format | Rows | Columns | Purpose |
| --- | --- | ---: | ---: | --- |
| `jobs.csv` | CSV | 500 | 5 | Job requisition master data |
| `candidates.json` | JSON array | 2001 | 6 | Candidate profile data |
| `education.csv` | CSV | 2000 | 4 | Candidate education records |
| `applications.csv` | CSV | 5000 | 4 | Link between candidates and jobs |
| `workflow_events.jsonl` | JSON Lines | 16769 | 4 | Application status event stream |

## File-Level Findings

### `jobs.csv`

Columns:

| Column | Meaning | Findings | Cleanup / Modeling |
| --- | --- | --- | --- |
| `job_id` | Job primary key | 500 unique values, no nulls | Use as job dimension key |
| `title` | Job title | No nulls | Trim whitespace |
| `department` | Business department | 48 nulls | Normalize case, preserve null or map to `UNKNOWN` depending on reporting needs |
| `posted_date` | Job posted date | No nulls, mixed date formats | Parse with reusable date macro |
| `status` | Job status | Values: `Open`, `Closed`, `Draft` | Normalize to uppercase and validate accepted values |

Date format distribution for `posted_date`:

| Format | Count |
| --- | ---: |
| `YYYY-MM-DD` | 423 |
| `YYYY.MM.DD` | 25 |
| `YYYY/MM/DD` | 18 |
| `DD-Mon-YYYY` | 18 |
| `Month D, YYYY` | 16 |


Key cleanup required:

- Parse `posted_date`.
- Normalize `department` and `status` to uppercase.
- Add dbt tests for `job_id` uniqueness and non-null `posted_date`.
- Decide how to handle missing `department`; current pipeline preserves the null

### `applications.csv`

Columns:

| Column | Meaning | Findings | Cleanup / Modeling |
| --- | --- | --- | --- |
| `application_id` | Application primary key | 5000 unique values, no nulls | Use as fact table grain |
| `job_id` | Foreign key to jobs | All values exist in `jobs.csv` | Validate relationships in production |
| `candidate_id` | Foreign key to candidates | All values exist in `candidates.json` | Validate relationships in production |
| `apply_date` | Date candidate applied | No nulls, mixed date formats | Parse with reusable date macro |

Date format distribution for `apply_date`:

| Format | Count |
| --- | ---: |
| `YYYY-MM-DD` | 4226 |
| `DD-Mon-YYYY` | 216 |
| `Month D, YYYY` | 197 |
| `YYYY.MM.DD` | 183 |
| `YYYY/MM/DD` | 178 |

Relationship findings:

- 500 distinct jobs appear in applications.
- 1841 distinct candidates appear in applications.

Key cleanup required:

- Parse `apply_date`.
- Deduplicate by `application_id`.
- Keep `application_id` as the grain of `fct_applications`.
- Add foreign-key style tests or relationship checks for `job_id` and `candidate_id`.

### `education.csv`

Columns:

| Column | Meaning | Findings | Cleanup / Modeling |
| --- | --- | --- | --- |
| `candidate_id` | Candidate key | 2000 unique values, no nulls | Join to candidate dimension |
| `degree` | Degree type | Values: `BS`, `MS`, `PhD` | Validate accepted values |
| `institution` | School name | No nulls | Trim whitespace |
| `year` | Graduation year | No nulls | Validate reasonable range in production |

Degree distribution:

| Degree | Count |
| --- | ---: |
| BS | 683 |
| MS | 675 |
| PhD | 642 |

Relationship findings:

- All education `candidate_id` values exist in `candidates.json`.
- One candidate profile does not have an education row.

Key cleanup required:

- Normalize `degree`.
- Deduplicate by `candidate_id`.
- Add accepted-values test for degree.
- Preserve candidates without education through a left join in `dim_candidate`.

### `candidates.json`

Columns:

| Column | Meaning | Findings | Cleanup / Modeling |
| --- | --- | --- | --- |
| `candidate_id` | Candidate primary key | 2001 unique values, no nulls | Use as candidate dimension key |
| `first_name` | Candidate first name | No nulls | Trim whitespace |
| `last_name` | Candidate last name | No nulls | Trim whitespace |
| `email` | Candidate email | No nulls, no invalid simple email patterns found | Lowercase and validate format |
| `phone` | Candidate phone | No nulls | Treat as PII in production |
| `skills` | List of skills |  | Convert to string for local DuckDB or normalize to bridge table in production |


Relationship findings:

- 160 candidate profiles have no application record.
- One candidate profile has no education record.

Key cleanup required:

- Lowercase `email`.
- Trim name and phone fields.
- Convert `skills` arrays to comma-separated strings for this local assignment.
- In production, consider a normalized `candidate_skill` bridge table for skill analytics.
- Treat email and phone as PII; apply masking/governance in a production lakehouse.

### `workflow_events.jsonl`

Columns:

| Column | Meaning | Findings | Cleanup / Modeling |
| --- | --- | --- | --- |
| `application_id` | Application key | 5000 distinct applications, all exist in `applications.csv` | Use to join workflow to applications |
| `old_status` | Previous status | 5000 nulls for first `Applied` event | Null is expected for first lifecycle event |
| `new_status` | New status | No nulls | Normalize and validate accepted values |
| `event_timestamp` | Event time | Mostly `YYYY-MM-DD`; one ISO timestamp with time component | Parse as timestamp |

Status distribution:

| `new_status` | Count |
| --- | ---: |
| Applied | 5000 |
| Screening | 3811 |
| Interview | 2646 |
| Offer | 1566 |
| Withdrawn | 1263 |
| Rejected | 1250 |
| Hired | 1233 |

`old_status` distribution:

| `old_status` | Count |
| --- | ---: |
| Null | 5000 |
| Applied | 4588 |
| Screening | 3360 |
| Interview | 2255 |
| Offer | 1566 |

Event timestamp findings:

- 16768 records use `YYYY-MM-DD`.
- 1 record uses an ISO timestamp with time component: `2025-11-08T00:00:00`.

Key cleanup required:

- Parse `event_timestamp` as timestamp.
- Normalize statuses to uppercase.
- Generate a surrogate event key because the file has no natural event ID.
- Deduplicate by `application_id`, `event_timestamp`, and `new_status`.
- Flag hired-before-applied anomalies.


## Data Quality Rules Derived From Analysis

Identified dbt/source quality checks:

- `jobs.job_id` is unique and not null.
- `jobs.posted_date` parses successfully.
- `jobs.status` is one of `OPEN`, `CLOSED`, `DRAFT`.
- `applications.application_id` is unique and not null.
- `applications.apply_date` parses successfully.
- `applications.job_id` exists in jobs.
- `applications.candidate_id` exists in candidates.
- `candidates.candidate_id` is unique and not null.
- `candidates.email` has a valid format.
- `education.candidate_id` is unique and not null for the current dataset.
- `education.degree` is one of `BS`, `MS`, `PHD`.
- `workflow_events.event_timestamp` parses successfully.
- `workflow_events.new_status` is one of `APPLIED`, `SCREENING`, `INTERVIEW`, `OFFER`, `WITHDRAWN`, `REJECTED`, `HIRED`.
- `workflow_events.application_id` exists in applications.
- Hired events cannot occur before apply date.

## Modeling Decisions Based On Source Analysis

- Use `application_id` as the grain of `fct_applications`.
- Use `event_id = md5(application_id || event_timestamp || new_status)` for workflow events.
- Keep raw data faithful to source and perform parsing/cleanup in dbt staging.
- Use a reusable dbt macro for mixed date parsing.
- Preserve null departments
- Use a left join from candidates to education so candidates without education are not dropped.
- Preserve anomalies and flag them instead of deleting them.