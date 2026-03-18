# BIRD Benchmark for NL2SQL++

This repository adapts mini_bird (a subset of the BIRD benchmark for Natural Language to SQL) to evaluate Natural Language to SQL++ conversion using Couchbase iQ.

## Overview

This project benchmarks Couchbase iQ's performance on the BIRD dataset by converting natural language questions into SQL++ queries and validating their correctness. Each natural language question, along with its evidence statement, is sent to iQ through cbq using the `USING AI` command, which internally calls OpenAI's GPT-4o model (at this point in time) to generate SQL++ statements.

### Command Structure
```
USING AI WITH {"keyspaces":[],"execute":true/false} <prompt>; Q:<question> H:<evidence>
```

The command returns a JSON object containing the `generated_statement`, which is then executed against the Couchbase database. Results are stored as CSV files for validation against gold standard results from the BIRD benchmark.

## Setup

### Dataset

All natural language questions with evidence statements and gold queries are stored in `mini_dev_data/mini_dev_sqlite.json`. This file is included in the repository and **must be used** for all questions and evidence statements, as we have corrected several inconsistencies found in the original dataset and ensured that all necessary information for the LLM is properly formatted and included in the prompt.

**Required**: You must download the complete mini_bird dataset from the following link because the SQL database files are essential for:
1. Converting SQLite databases to JSON format for Couchbase loading
2. Generating gold standard CSV results for validation
3. Completing the full data pipeline

Download link: [https://drive.google.com/file/d/13VLWIwpw5E3d5DUkMvzw7hvHE67a4XkG/view](https://drive.google.com/file/d/13VLWIwpw5E3d5DUkMvzw7hvHE67a4XkG/view)

After downloading, place the extracted dataset folder inside this repository root so scripts can locate the database files using the expected relative paths.

**Important**: While the dataset download is mandatory for the database files, always use the questions and evidence from our `mini_dev_data/mini_dev_sqlite.json` file in this repository rather than the original dataset files. Our version contains corrections and ensures all contextual information needed by the LLM is properly included in the prompts.

### Prerequisites
The evaluation compares CSV outputs from two sources:
- **Test CSV**: Results from executing iQ-generated SQL++ queries
- **Gold CSV**: Results from executing the original SQL queries from BIRD benchmark

**Important**: Before running any scripts, verify and update file paths in the scripts according to your local directory structure. Key paths to check include:
- Database file locations in `run_sql_gold_queries.py`, `transform_sqlite_to_json.py`
- Output directories in all scripts (`testIQ.py`, `compare_results.py`, `pipeline.py`)
- JSON data file paths in `batch_import_to_couchbase.py` and other scripts
- Configuration file paths (`IQ_config.json`)

### Data Pipeline

**Step 1: Convert SQLite to JSON**
```bash
python Scripts/sqlLiteToJson/transform_sqlite_to_json.py
```
This converts SQLite database files from the BIRD benchmark into JSON format suitable for Couchbase import.

**Step 2: Load Data into Couchbase**
```bash
python Scripts/loadDataToCB/batch_import_to_couchbase.py
```
This batch imports all JSON files into a locally running Couchbase server, creating buckets, scopes, and collections automatically.

**Step 3: Generate Gold Standard Results**
```bash
python Scripts/comparingResults/run_sql_gold_queries.py
```
This executes all original SQL queries from the BIRD benchmark against the SQLite databases and saves the results as CSV files in `Scripts/comparingResults/2sql_query_results/`. These gold standard CSV files are used as the ground truth for comparison when validating iQ-generated SQL++ queries.

**Step 4: Configuration**

Create an `IQ_config.json` file in the root directory with the following structure:

```json
{
  "_comment": "Couchbase IQ BIRD Evaluation Configuration",
  "couchbase": {
    "cb_endpoint": "127.0.0.1:9000",
    "username": "Administrator",
    "password": "password"
  },
  "couchbase_iq": {
    "_comment": "Get these credentials from your Capella account",
    "natural_orgid": "<your_org_id>",
    "natural_cred": "<your_capella_email>:<capella_password>"
  },
  "evaluation": {
    "_comment": "Configuration for test execution",
    "max_questions": null,
    "save_sql": true,
    "execute_sql": true,
    "delay_between_requests": 1,
    "rerun_existing": false,
    "skip_failed_last_run": false
  }
}
```

**Management Port Requirement (`cb_endpoint`):**
- `cb_endpoint` must point to **host:management-port** (not Query port, not KV port).
- For Couchbase Server app/local install, use management port **8091** (example: `127.0.0.1:8091`).
- For `cluster_run`, use management port **9000** (example: `127.0.0.1:9000`).
- Scripts will discover node services from `cb_endpoint` and automatically resolve the correct Query endpoint (`n1ql` port) at runtime.

**Configuration Options:**
- `max_questions`: Set to a number (e.g., 2) for testing, or `null` to process all questions
- `rerun_existing`: `true` to re-run already processed questions, `false` to skip them (this just checks if output for a particular query already exists in the output folder)
- `skip_failed_last_run`: `true` to skip questions that failed in the previous run

## Pipeline: Two-Pass Approach

### `Scripts/pipeline.py`

The pipeline implements a two-pass strategy to maximize successful SQL++ generation. This is the **main script** you should run - it combines the functionality of individual components (SQL generation and result comparison) into a complete workflow.

**Optimized Prompts:** The initial and retry prompts configured in `pipeline.py` have been fine-tuned through extensive testing and represent the best-performing prompt templates for this benchmark. These prompts include specific rule sets, constraints, and guidance that achieved the highest success rates in our evaluations.

**Pass 1: Initial Generation**
1. Runs `testIQ.py` logic to generate SQL++ for all questions using an initial prompt
2. Executes generated queries and saves results to `baseline8/` directory
3. Runs `compare_results.py` logic to identify which queries failed validation
4. Collects failed query IDs for retry

**Pass 2: Retry with Enhanced Prompt**
1. Runs `testIQ.py` logic again, but only for failed queries from Pass 1
2. Uses a modified retry prompt with additional constraints and guidance
3. Executes newly generated queries and saves results to `baseline8_retry/` directory
4. Runs `cmpare_results.py` logic on retry results

**Final Results:**
The pipeline combines successful queries from both passes:
- Initial successes (queries that passed in Pass 1)
- Retry successes (queries that failed in Pass 1 but passed in Pass 2)

This multi-pass approach improves overall accuracy by giving the model a second chance with refined prompting for queries it initially struggled with.

**Features:**
- Skips already-processed questions (checks for existing `.sql` and `.csv` files)
- Comprehensive logging of all IQ API calls with request/response details
- Detailed error handling with visibility into retry logic and ADVISE mechanism
- Separate tracking of generation statistics (success/failed/skipped) and comparison results (files compared/passed/failed)
- Final summary combining both passes for complete accuracy metrics
- **ORDER BY-aware comparison**: automatically detects which gold queries contain `ORDER BY` and enforces strict row-order matching for those queries in both Pass 1 and Pass 2 comparisons

> **Note:** Individual components (`testIQ.py` and `compare_results.py`) are explained in detail in the [Core Scripts](#core-scripts) section below and are available if you need to run them separately for debugging or custom workflows.

## Core Scripts

### SQL Generation: `Scripts/testIQ.py`

This script handles the SQL++ generation process:
1. Loads question data from `mini_dev_sqlite.json`
2. Constructs prompts with question and evidence
3. Sends prompts to Couchbase iQ via the `USING AI` command
4. Receives generated SQL++ statements
5. Executes the statements against Couchbase
6. Saves both the SQL++ query (`.sql` files) and results (`.csv` files)

**Features:**
- Skips already-processed questions when `rerun_existing=false`
- Includes retry logic with error handling
- Implements ADVISE mechanism for query optimization when certain errors occur
- Logs detailed execution information for debugging

#### Error Handling & Recovery Mechanisms

The pipeline implements error handling system designed to maximize query success rates:

**1. Retry Logic**
- **Configurable Retries:** Up to 5 retry attempts per query with exponential backoff
- **Error Code Recognition:** Specific handling for known error patterns (19212, 19214, etc.)

**2. Adaptive Prompt Modification**
The system automatically modifies prompts based on specific error codes:
- **Error 19212:** Adds guidance text about using only provided keyspaces and fields
- **Error 19214:** Adds constraints about avoiding correlated subqueries
- **Statement Enhancement:** Dynamically appends error-specific instructions to improve success rates

**3. ADVISE-Based Query Optimization**
For persistent Error 19214 (missing indexes), the system implements automatic index creation:
- **ADVISE Query Execution:** Runs `ADVISE <extracted_sql>` to get index recommendations
- **Dynamic Index Creation:** Creates covering indexes and secondary indexes as recommended
- **Query Retry:** Re-executes the original query with optimized indexes
- **Cleanup:** Automatically removes created indexes after query completion
- **SQL Extraction:** Automatically extracts generated SQL from error messages using regex patterns

**4. Error Code Handling Matrix**
```
Error 19212: Missing keyspaces/fields → Prompt enhancement → Retry
Error 19214: Missing indexes → ADVISE mechanism → Index creation → Retry
```

### Result Comparison: `Scripts/comparingResults/compare_results.py`

This script validates the correctness of iQ-generated queries by comparing test CSV files (from SQL++ execution) against gold CSV files (from original SQL queries). The comparison uses intelligent column mapping and strict row validation.

#### Comparison Logic

**Value Normalization:**
Before comparison, all values are normalized to prevent false mismatches. Strings are trimmed and lowercased (`"  Hello  "` → `"hello"`), null representations are standardized (`null`, `None`, `NaN`, empty string, `n/a` → `None`), and numbers are normalized (floats rounded to 6 decimals, whole floats like `42.0` converted to `int 42`). This ensures consistency across different database formatting conventions.

**Type Detection:**
Each column is analyzed by sampling up to 100 values to classify it as `numeric` (≥80% parseable as numbers), `string` (≥80% text), or `mixed` (neither threshold met). This type awareness prevents nonsensical mappings, ensuring numeric columns only map to numeric columns and string columns to string columns. Mixed-type columns can map to any type as a fallback.

**Hybrid Column Mapping - Stage 1 (Fuzzy Matching):**
The first mapping stage uses fuzzy string matching with a 70% similarity threshold to find semantically similar column names. This catches variations like `"user_id"` ↔ `"userId"` (100% similar), `"email_addr"` ↔ `"email"` (90% similar), or `"first_name"` ↔ `"fname"` (78% similar), while rejecting weak matches like `"total"` ↔ `"count"` (43% similar). Type compatibility is strictly enforced—only columns with matching types can be paired. Successfully matched columns are locked and excluded from the next stage.

**Hybrid Column Mapping - Stage 2 (Positional Fallback):**
For columns unmatched by name similarity, the script uses positional matching, pairing unmapped columns by their left-to-right order. Unlike naive positional matching, this stage still enforces type compatibility: the first unmapped gold column matches the first type-compatible unmapped test column. If types don't match, it tries the next unmapped test column. This prevents mapping `"age"` (numeric, position 3) to `"city"` (string, position 3) based solely on position.

**DataFrame Alignment:**
After establishing the column mapping, the test dataframe is transformed to match the gold schema. Test columns are renamed according to the mapping (e.g., `"userId"` → `"user_id"`), unmapped test columns are dropped (extra columns are acceptable and ignored), only gold schema columns are retained, and columns are reordered to match gold column order. This ensures proper value alignment during row comparison.

**Row Comparison:**
Rows are compared by converting each row to a tuple of values. The comparison mode depends on whether the gold SQL query contains an `ORDER BY` clause:

- **Order-insensitive mode** (default, no `ORDER BY`): Row tuples are collected into sets and compared with set subtraction, so row order is irrelevant. Missing rows (in gold but not in test) and extra rows (in test but not in gold) are both tracked. A fallback method treats rows as frozensets (value-order-independent within each row) when tuple set comparison fails but row counts match, catching cases where column mapping wasn't perfect but data is present.

- **Order-sensitive mode** (`ORDER BY` detected in gold SQL): The ordered list of row tuples must match exactly between gold and test—same rows in the same sequence. This prevents a wrongly ordered result from being marked correct. Row content mismatches (missing or extra rows) still cause failure.

The script pre-loads `mini_dev_sqlite.json` at startup to identify which query IDs have `ORDER BY` in their gold SQL, and applies the appropriate comparison mode automatically for each query.

In both modes the validation is strict on row content: test must have EXACTLY the same rows as gold—zero missing rows AND zero extra rows.

**Success Classification:**
A comparison succeeds when test has exactly the same rows as gold (no missing, no extra), regardless of column name or order differences handled by mapping. An "exact match" is a success where test also has the same column count as gold (no extra columns). Failure occurs with missing rows, extra rows, or complete column mapping failure. The validation is strict on row content but flexible on schema differences.

**Output Reports:**
The script generates multiple outputs: console and log file summaries with statistics (overall accuracy, generation rate, success rate), a JSON report with detailed per-query results and mapping information, a CSV report for spreadsheet analysis, and individual detailed failure logs (JSON files per failed query) with sample mismatched rows, full column mapping breakdown, and debugging metrics.

## Benchmark Results

This section documents the performance of Couchbase iQ on the BIRD mini_dev dataset using the two-pass pipeline approach.

### Evaluation Metrics

**ORDER BY Check in Evaluation:**
- The evaluator inspects each gold SQL query for `ORDER BY`.
- If `ORDER BY` is present, comparison is **order-sensitive** (row sequence must match exactly).
- If `ORDER BY` is absent, comparison is **order-insensitive** (set-based row equality).
- In both cases, row content is strict: no missing rows and no extra rows.

**Pass 1: Initial Generation**
- Total Questions: 500
- Successful Queries: 333
- Failed Queries: 167
- Success Rate: 66.6%

**Pass 2: Retry with Enhanced Prompt**
- Questions Retried: 167 (all failures from Pass 1)
- Additional Successes: 56
- Still Failing: 111
- Retry Success Rate: 33.5%

### Final Results

| Metric | Value |
|--------|-------|
| Total Questions | 500 |
| Successful Queries | 389 |
| Failed Queries | 111 |
| **Overall Accuracy** | **77.80%** |

**Breakdown:**
- 333 queries succeeded on first attempt (67.0%)
- 56 queries succeeded on retry after initial failure (9.8%)
- 111 queries failed both attempts (22.2%)

