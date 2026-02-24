# BIRD Benchmark for NL2SQL++

This repository adapts mini_bird (a subset of the BIRD benchmark for Natural Language to SQL) to evaluate Natural Language to SQL++ conversion using Couchbase iQ.

## Overview

This project benchmarks Couchbase iQ's performance on the BIRD dataset by converting natural language questions into SQL++ queries and validating their correctness. Each natural language question, along with its evidence statement, is sent to iQ through cbq using the `USING AI` command, which internally calls OpenAI's GPT-4o model to generate SQL++ statements.

### Command Structure
```
USING AI WITH {"keyspaces":[],"execute":true/false} <prompt>; Q:<question> H:<evidence>
```

The command returns a JSON object containing the `generated_statement`, which is then executed against the Couchbase database. Results are stored as CSV files for validation against gold standard results from the BIRD benchmark.

## Setup

### Prerequisites
The evaluation compares CSV outputs from two sources:
- **Test CSV**: Results from executing iQ-generated SQL++ queries
- **Gold CSV**: Results from executing the original SQL queries from BIRD benchmark

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

**Step 3: Configuration** 

**Step 3: Configuration**

Create an `IQ_config.json` file in the root directory with the following structure:

```json
{
  "_comment": "Couchbase IQ BIRD Evaluation Configuration",
  "couchbase": {
    "connection_string": "couchbase://localhost",
    "username": "Administrator",
    "password": "password",
    "query_endpoint": "http://localhost:8093/query/service"
  },
  "couchbase_iq": {
    "_comment": "Get these credentials from your Capella account",
    "natural_orgid": "<your_org_id>",
    "natural_cred": "<your_credentials>"
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

**Configuration Options:**
- `max_questions`: Set to a number (e.g., 2) for testing, or `null` to process all 547 questions
- `rerun_existing`: `true` to re-run already processed questions, `false` to skip them
- `skip_failed_last_run`: `true` to skip questions that failed in the previous run

### Dataset

All natural language questions with evidence statements and gold queries are stored in `mini_dev_data/mini_dev_sqlite.json`. This file has been formatted to include all information sent to the LLM in the prompt.

**Download Full Dataset**: The complete mini_bird dataset and database descriptor files are available at:
[https://drive.google.com/file/d/13VLWIwpw5E3d5DUkMvzw7hvHE67a4XkG/view](https://drive.google.com/file/d/13VLWIwpw5E3d5DUkMvzw7hvHE67a4XkG/view)

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
Rows are compared by converting each row to a tuple of values and creating sets of row-tuples for both dataframes. The comparison identifies missing rows (in gold but not in test) and extra rows (in test but not in gold). The validation is strict: test must have EXACTLY the same rows as gold—zero missing rows AND zero extra rows. Even if test contains all gold rows, having additional rows causes failure as it indicates incorrect query logic (wrong WHERE clause, JOIN, or aggregation). A fallback method treats rows as frozensets (order-independent) when tuple comparison fails but row counts match, catching cases where column mapping wasn't perfect but data is present.

**Success Classification:**
A comparison succeeds when test has exactly the same rows as gold (no missing, no extra), regardless of column name or order differences handled by mapping. An "exact match" is a success where test also has the same column count as gold (no extra columns). Failure occurs with missing rows, extra rows, or complete column mapping failure. The validation is strict on row content but flexible on schema differences.

**Output Reports:**
The script generates multiple outputs: console and log file summaries with statistics (overall accuracy, generation rate, success rate), a JSON report with detailed per-query results and mapping information, a CSV report for spreadsheet analysis, and individual detailed failure logs (JSON files per failed query) with sample mismatched rows, full column mapping breakdown, and debugging metrics.

## Pipeline: Two-Pass Approach

### `Scripts/pipeline.py`

The pipeline implements a two-pass strategy to maximize successful SQL++ generation:

**Pass 1: Initial Generation**
1. Runs `testIQ.py` logic to generate SQL++ for all questions using an initial prompt
2. Executes generated queries and saves results to `baseline8/` directory
3. Runs `compare_results.py` logic to identify which queries failed validation
4. Collects failed query IDs for retry

**Pass 2: Retry with Enhanced Prompt**
1. Runs `testIQ.py` logic again, but only for failed queries from Pass 1
2. Uses a modified retry prompt with additional constraints and guidance
3. Executes newly generated queries and saves results to `baseline8_retry/` directory
4. Runs `compare_results.py` logic on retry results

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
 