#!/usr/bin/env python3
"""
Self-contained Pipeline Script
Orchestrates two-pass SQL generation and validation workflow
All logic embedded - no external script dependencies
"""

import json
import sys
import csv
import re
import time
import os
import pandas as pd
import requests
from pathlib import Path
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Set, Tuple
from collections import defaultdict, Counter
from difflib import SequenceMatcher
from couchbase_endpoint_utils import resolve_query_endpoint_from_config

# ============================================================================
# CONFIGURABLE PROMPTS - EDIT THESE AS NEEDED
# ============================================================================

# First run prompt - used for initial query generation
INITIAL_PROMPT = "Generate a syntactically correct query under these mandatory constraints: use only provided keyspaces and existing fields; never use *;Main SELECT must NOT use RAW and must return standard field expressions only; RAW is allowed only inside SELECT subqueries.; subquery value access must use IN (SELECT RAW ...) or =(SELECT RAW ...)[0]; When calculating percentages, for any SELECT RAW used in the denominator, the subquery MUST be immediately followed by [0]; NO correlated subqueries (use JOIN); do not repeat aliases and alias must not equal bucket name; no DIVIDE or SUBTRACT; use DISTINCT and handle NULL when needed; do not add ':' after FROM; return only what is asked. Q:{question} Hint:{evidence}"

# Retry run prompt - used for failed queries
RETRY_PROMPT = "Generate a syntactically correct query that MUST follow ALL rules: Only use functions supported by CouchBase N1QL; do not add ':' after FROM; PERCENTAGE RULE (MANDATORY): numerator and denominator MUST be computed from the SAME row set; computing the denominator using a separate subquery is FORBIDDEN; define a single base set (via FROM/JOIN/WHERE) representing the full population, compute the denominator from this base set, and derive the numerator ONLY by adding conditions inside aggregate expressions (e.g., CASE WHEN); GROUP BY should not be used when only one keyspace is involved; Use only existing fields in the collections, DO NOT Hallucinate and refer to fields that do not exist; Make sure to use fields from the Correct Collections; Q: {question} Hint: {evidence}"

# ============================================================================
# PIPELINE CONFIGURATION
# ============================================================================

SCRIPT_DIR = Path(__file__).parent
CONFIG_FILE = SCRIPT_DIR.parent / "IQ_config.json"
QUESTIONS_FILE = SCRIPT_DIR.parent / "mini_dev_data" / "mini_dev_sqlite.json"

# Specify the path for the file where the json format of the sqlite databases is stored after conversion (used to get the keyspace and field info for the prompt)
JSON_OUTPUT_DIR = SCRIPT_DIR / "sqlLiteToJson" / "json_outputs"

# Specify path for the gold results to compare against (in csv format)
SQLITE_RESULTS_DIR = SCRIPT_DIR / "comparingResults" / "sql_query_results"

# Specify path where results of the pipeline will be stored (SQL files, CSV outputs, logs)
INITIAL_RUN_BASELINE = "baseline12"
RETRY_RUN_BASELINE = "baseline12_retry"

PIPELINE_LOG = SCRIPT_DIR / "pipeline_results.log"

# Default Configuration
DEFAULT_CONFIG = {
    "couchbase": {
        "cb_endpoint": "127.0.0.1:9000",
        "username": "Administrator",
        "password": "password"
    },
    "couchbase_iq": {
        "natural_cred": "<capella_username:password>",
        "natural_orgid": "<capella_orgid>"
    }
}

# ============================================================================
# UTILITY FUNCTIONS
# ============================================================================

def get_ist_timestamp():
    """Get current time in IST (UTC+5:30)"""
    utc_now = datetime.utcnow()
    ist_offset = timedelta(hours=5, minutes=30)
    ist_now = utc_now + ist_offset
    return ist_now.strftime("%Y-%m-%d %H:%M:%S IST")

def log_print(message: str):
    """Print to console and log file."""
    print(message)
    with open(PIPELINE_LOG, 'a') as f:
        f.write(message + '\n')

def write_pipeline_header():
    """Write pipeline run header to log."""
    with open(PIPELINE_LOG, 'a') as f:
        f.write('\n' + '='*80 + '\n')
        f.write(f"PIPELINE RUN STARTED: {get_ist_timestamp()}\n")
        f.write('='*80 + '\n')

def load_config():
    """Load configuration from json file or use defaults"""
    if CONFIG_FILE.exists():
        with open(CONFIG_FILE, "r") as f:
            return json.load(f)
    else:
        return DEFAULT_CONFIG


def load_questions(json_path: Path = None) -> List[Dict]:
    """Load questions from JSON file."""
    if json_path is None:
        json_path = QUESTIONS_FILE

    with open(json_path, "r") as f:
        dataset = json.load(f)

    questions = []
    for ex in dataset:
        question_entry = {
            "question_id": ex['question_id'],
            "question": ex['question'],
            "evidence": ex.get('evidence', None),
            "db_id": ex['db_id']
        }
        questions.append(question_entry)
    return questions


def has_order_by_clause(sql_query: str) -> bool:
    """Return True if SQL text contains ORDER BY (case-insensitive)."""
    if not sql_query or not isinstance(sql_query, str):
        return False
    return re.search(r'\border\s+by\b', sql_query, flags=re.IGNORECASE) is not None


def load_order_sensitive_query_ids(json_path: Path = None) -> Set[str]:
    """Load question IDs whose gold SQL contains ORDER BY."""
    if json_path is None:
        json_path = QUESTIONS_FILE

    order_sensitive_ids = set()

    try:
        with open(json_path, "r") as f:
            dataset = json.load(f)

        for ex in dataset:
            qid = ex.get('question_id')
            sql_text = ex.get('SQL', '')
            if qid is not None and has_order_by_clause(sql_text):
                order_sensitive_ids.add(str(qid))
    except Exception as e:
        log_print(f"Warning: Could not load ORDER BY metadata from {json_path}: {e}")

    return order_sensitive_ids

# ============================================================================
# COUCHBASE IQ API CLASS (with ADVISE logic)
# ============================================================================

class CouchbaseIqAPI:
    def __init__(self, query_endpoint, natural_cred, natural_orgid, couchbase_user, couchbase_pass, prompt_template):
        self.query_endpoint = query_endpoint
        self.natural_cred = natural_cred
        self.natural_orgid = natural_orgid
        self.couchbase_user = couchbase_user
        self.couchbase_pass = couchbase_pass
        self.prompt_template = prompt_template
        
        # Error code -> additional statement text mapping
        self.error_code_statement_additions = {
            19212: "I know that the required information exists in the provided keyspaces and fields. Please try harder and generate a correct SQL++ query using only the provided keyspaces and fields. Remember to follow all the constraints mentioned earlier and return only what is asked in the question.",
            19214 : "Don't use Corelated subqueries. All subqueries must be non-correlated. A subquery must not reference outer query aliases or columns. Each query block must use only its own locally defined aliases."
        }

    def generate_sql(self, keyspaces: List[str], question: str, evidence: Optional[str] = None, max_retries: int = 5) -> tuple:
        """Generate SQL using Couchbase IQ with ADVISE logic."""
        print(f"================================================= SS ================================================")
        with_params = {
            "keyspaces": keyspaces,
            "execute": False
        }

        with_clause_json = json.dumps(with_params)
        statement = f"USING AI WITH {with_clause_json}. {self.prompt_template.format(question=question, evidence=evidence or '')}"
        
        print(f"SS:  Question: {question}")
        print(f"SS:  External Knowledge: {evidence}")
        print(f"SS:  Statement: {statement}")

        payload = {
            "statement": statement,
            "natural_cred": self.natural_cred,
            "natural_orgid": self.natural_orgid
        }
        
        headers = {
            "Content-Type": "application/json",
            "Authorization": requests.auth._basic_auth_str(self.couchbase_user, self.couchbase_pass)
        }

        print(f"SS:  Payload: {payload}")
        print(f"SS:  Headers: {headers}")
        print(f"SS:  Query Endpoint: {self.query_endpoint}")

        handled_error_codes = set()
        
        for attempt in range(1, max_retries + 1):
            response = None
            try:
                if attempt > 1:
                    print(f"SS:  Retry attempt {attempt}/{max_retries}...")
                else:
                    print("SS:  Sending request to Couchbase IQ...")
                
                response = requests.post(self.query_endpoint, json=payload, headers=headers)
                response.raise_for_status()
                print(f"SS: Response: {response}")
                data = response.json()
                print(f"SS: Data: {data}")
                print(f"================================================= SS ================================================")
                return (data.get("generated_statement", ""), statement)
                
            except requests.exceptions.RequestException as e:
                error_code = None
                error_msg = ""
                
                if response is not None:
                    try:
                        error_data = response.json()
                        if "errors" in error_data and len(error_data["errors"]) > 0:
                            error_code = error_data["errors"][0].get("code")
                            error_msg = error_data["errors"][0].get("msg", "")
                            
                            print(f"  ✗ Error generating SQL - Code: {error_code} (Attempt {attempt}/{max_retries})")
                            print(f"     Message: {error_msg[:200]}...")
                            
                            # Add statement modification for error code
                            if error_code in self.error_code_statement_additions and error_code not in handled_error_codes:
                                additional_text = self.error_code_statement_additions[error_code]
                                statement = statement + additional_text
                                payload["statement"] = statement
                                handled_error_codes.add(error_code)
                                print(f"  ⚡ Error {error_code} detected - Statement modified")
                                print(f"     Added text: {additional_text[:100]}...")
                                print(f"     Modified statement will be sent on next retry")
                                time.sleep(1)
                                continue
                            
                            # ADVISE logic for persistent 19214 error
                            if error_code == 19214 and error_code in handled_error_codes and f"{error_code}_ADVISE_ATTEMPTED" not in handled_error_codes:
                                print(f"  ⚡ Error 19214 persists - attempting ADVISE approach...")
                                extracted_sql = self._extract_sql_from_error(error_msg)
                                
                                if extracted_sql:
                                    print(f"  ✓ Extracted SQL from error message ({len(extracted_sql)} chars)")
                                    print(f"  🔍 Running ADVISE query...")
                                    advise_results = self._execute_query(f"ADVISE {extracted_sql}")
                                    
                                    if advise_results and "results" in advise_results and advise_results["results"]:
                                        advice_data = advise_results["results"][0]
                                        if "advice" in advice_data:
                                            print(f"  📝 Executing index recommendations...")
                                            index_success, created_indexes = self._execute_index_recommendations(advice_data["advice"])
                                            if index_success:
                                                print(f"  🔄 Retrying original query after index creation...")
                                                final_results = self._execute_query(extracted_sql)
                                                print(f"  🗑️ Cleaning up created indexes...")
                                                self._delete_indexes(created_indexes)
                                                
                                                if final_results and final_results.get("status") == "success":
                                                    print(f"  ✓ ADVISE approach successful!")
                                                    return (extracted_sql, statement)
                                                else:
                                                    print(f"  ✗ Query retry failed after ADVISE")
                                            else:
                                                print(f"  ✗ Failed to create indexes")
                                        else:
                                            print(f"  ✗ No advice in ADVISE results")
                                    else:
                                        print(f"  ✗ ADVISE query failed or returned no results")
                                else:
                                    print(f"  ✗ Could not extract SQL from error message")
                                
                                handled_error_codes.add(f"{error_code}_ADVISE_ATTEMPTED")
                            
                            # Extract SQL from error for 19214/19212 after 5 attempts
                            if error_code in [19214, 19212] and attempt >= 5:
                                print(f"  🔄 Attempt {attempt}: Trying SQL extraction fallback...")
                                extracted_sql = self._extract_sql_from_error(error_msg)
                                if extracted_sql:
                                    print(f"  ✓ Extracted SQL from error message ({len(extracted_sql)} chars)")
                                    return (extracted_sql, statement)
                                else:
                                    print(f"  ✗ SQL extraction fallback failed")
                    except json.JSONDecodeError:
                        print(f"  ✗ Request failed - Could not parse error response (Attempt {attempt}/{max_retries})")
                        pass
                else:
                    print(f"  ✗ Request failed - No response received (Attempt {attempt}/{max_retries})")
                    print(f"     Exception: {str(e)[:200]}...")
                
                if attempt < max_retries:
                    print(f"  🔄 Retrying in 1 second...")
                    time.sleep(1)
        
        print(f"  ✗ All {max_retries} attempts failed")
        print(f"================================================= SS ================================================")
        return ("", statement)


    # redundant right now (initially used to extract SQL++ from error messages produced to error code 19214, but now we take the approach of running ADVISE and creating the indexes)
    def _extract_sql_from_error(self, error_msg: str) -> Optional[str]:
        """Extract SQL from error message."""
        try:
            sql_match = re.search(r'```\s*(.*?)\s*```', error_msg, re.DOTALL)
            if sql_match:
                return sql_match.group(1).strip()
            
            sql_match = re.search(r'```\s*\n?(.*?)$', error_msg, re.DOTALL)
            if sql_match:
                return sql_match.group(1).strip()
            
            sql_match = re.search(r'statement-\s*```\s*\n?(.*?)(?:\n```|$)', error_msg, re.DOTALL)
            if sql_match:
                return sql_match.group(1).strip()
        except Exception:
            pass
        return None
    
    def _execute_query(self, sql_query: str) -> Optional[Dict]:
        """Execute a SQL query."""
        payload = {
            "statement": sql_query,
            "timeout": "300s"
        }
        
        headers = {
            "Content-Type": "application/json",
            "Authorization": requests.auth._basic_auth_str(self.couchbase_user, self.couchbase_pass)
        }
        
        try:
            response = requests.post(self.query_endpoint, json=payload, headers=headers)
            if response.status_code != 200:
                print(f"     Query execution failed - Status: {response.status_code}")
                return None
            
            data = response.json()
            if data.get("status") == "fatal" or "errors" in data:
                if "errors" in data:
                    print(f"     Query execution error: {data['errors'][0].get('msg', '')[:100]}...")
                return None
            return data
        except Exception as e:
            print(f"     Query execution exception: {str(e)[:100]}...")
            return None
    
    def _execute_index_recommendations(self, advice_data: Dict) -> tuple:
        """Execute CREATE INDEX statements from ADVISE."""
        if not advice_data or "adviseinfo" not in advice_data:
            print(f"     No adviseinfo in advice data")
            return (False, [])
        
        advise_info = advice_data["adviseinfo"]
        if "recommended_indexes" not in advise_info:
            print(f"     No recommended_indexes in adviseinfo")
            return (False, [])
        
        recommended = advise_info["recommended_indexes"]
        created_indexes = []
        
        if "covering_indexes" in recommended:
            print(f"     Creating {len(recommended['covering_indexes'])} covering indexes...")
            for idx in recommended["covering_indexes"]:
                if "index_statement" in idx:
                    result = self._execute_query(idx["index_statement"])
                    if result and result.get("status") == "success":
                        index_info = self._extract_index_name(idx["index_statement"])
                        if index_info:
                            created_indexes.append(index_info)
                            print(f"     ✓ Created index: {index_info[0]}")
        
        if "indexes" in recommended:
            print(f"     Creating {len(recommended['indexes'])} indexes...")
            for idx in recommended["indexes"]:
                if "index_statement" in idx:
                    result = self._execute_query(idx["index_statement"])
                    if result and result.get("status") == "success":
                        index_info = self._extract_index_name(idx["index_statement"])
                        if index_info:
                            created_indexes.append(index_info)
                            print(f"     ✓ Created index: {index_info[0]}")
        
        print(f"     Total indexes created: {len(created_indexes)}")
        return (len(created_indexes) > 0, created_indexes)
    
    def _extract_index_name(self, create_statement: str) -> Optional[tuple]:
        """Extract index name and keyspace from CREATE INDEX statement."""
        match = re.search(r'CREATE\s+INDEX\s+(`?[\w]+`?)\s+ON\s+(`[^`]+`(?::`[^`]+`)*(?:\.`[^`]+`)*)\s*\(', 
                         create_statement, re.IGNORECASE)
        if match:
            index_name = match.group(1).strip('`')
            keyspace = match.group(2)
            return (index_name, keyspace)
        return None
    
    def _delete_indexes(self, index_info_list: List[tuple]) -> None:
        """Delete a list of indexes."""
        for index_name, keyspace in index_info_list:
            drop_statement = f"DROP INDEX `{index_name}` ON {keyspace}"
            print(f"     🗑️ Dropping index: {index_name}")
            self._execute_query(drop_statement)

# ============================================================================
# QUERY EXECUTION FUNCTIONS
# ============================================================================

def get_all_collections_in_scope(bucket_name: str, scope_name: str) -> Dict[str, List[str]]:
    """Get all collections in a scope along with their field names."""
    json_path = Path(JSON_OUTPUT_DIR) / f"{bucket_name}.json"

    if not json_path.exists():
        return {}

    try:
        with open(json_path, 'r') as f:
            data = json.load(f)
    except:
        return {}
    
    collections_with_fields = {}
    
    for table_name, table_data in data.items():
        if table_name == "sqlite_sequence":
            continue
        
        fields = []
        if isinstance(table_data, list) and len(table_data) > 0 and isinstance(table_data[0], dict):
            fields = list(table_data[0].keys())
        
        collections_with_fields[table_name] = fields
    
    return collections_with_fields

def get_keyspaces_for_db(db_id: str) -> List[str]:
    """Get keyspace names for a given database."""
    collections_dict = get_all_collections_in_scope(
        bucket_name=db_id,
        scope_name="bird", #redundant
    )
    
    selected_collections = list(collections_dict.keys())
    
    keyspaces = []
    for coll in selected_collections:
        keyspaces.append(f"`{db_id}`.`bird`.`{coll}`")
    
    return keyspaces

def exec_sql(sql_query: str, couchbase_user: str, couchbase_pass: str, query_endpoint: str) -> Optional[List[Dict]]:
    """Execute SQL query on Couchbase."""
    payload = {"statement": sql_query}
    
    headers = {
        "Content-Type": "application/json",
        "Authorization": requests.auth._basic_auth_str(couchbase_user, couchbase_pass)
    }
    
    try:
        response = requests.post(query_endpoint, json=payload, headers=headers)
        response.raise_for_status()
        data = response.json()
        return data.get("results", [])
    except:
        return []

def json_to_csv(json_results: List[Dict], output_file: Path) -> bool:
    """Convert JSON results to CSV format."""
    if not json_results:
        with open(output_file, 'w', newline='') as f:
            f.write('')
        return True
    
    try:
        all_keys = {}
        for result in json_results:
            if isinstance(result, dict):
                for key, value in result.items():
                    if isinstance(value, dict):
                        for nested_key in value.keys():
                            all_keys[f"{key}.{nested_key}"] = None
                    else:
                        all_keys[key] = None
        
        if not all_keys:
            with open(output_file, 'w', newline='') as f:
                f.write('')
            return True
        
        fieldnames = list(all_keys.keys())
        
        with open(output_file, 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            
            for result in json_results:
                row = {}
                for key, value in result.items():
                    if isinstance(value, dict):
                        for nested_key, nested_value in value.items():
                            row[f"{key}.{nested_key}"] = nested_value
                    else:
                        row[key] = value
                writer.writerow(row)
        
        return True
    except:
        return False

def process_question(
    item: Dict,
    couchbase_iq: CouchbaseIqAPI,
    couchbase_user: str,
    couchbase_pass: str,
    query_endpoint: str,
    output_sql_dir: Path,
    output_csv_dir: Path
) -> bool:
    """Process a single question: generate SQL, execute, save results."""
    qId = item['question_id']
    question = item['question']
    evidence = item.get('evidence', None)
    db_id = item['db_id']

    keyspaces = get_keyspaces_for_db(db_id)

    generated_sql, statement = couchbase_iq.generate_sql(
        keyspaces=keyspaces,
        question=question,
        evidence=evidence
    )

    if not generated_sql:
        return False

    # Save SQL
    sql_file = output_sql_dir / f"{qId}.sql"
    try:
        with open(sql_file, 'w') as f:
            f.write(generated_sql)
    except:
        return False
    
    # Execute SQL and save CSV
    results = exec_sql(
        sql_query=generated_sql,
        couchbase_user=couchbase_user,
        couchbase_pass=couchbase_pass,
        query_endpoint=query_endpoint
    )
    
    if results is None:
        return False
    
    csv_file = output_csv_dir / f"{qId}.csv"
    if not json_to_csv(results, csv_file):
        return False
    
    return True

# ============================================================================
# COMPARISON FUNCTIONS (from compare_results_better.py)
# ============================================================================

def normalize_value(val):
    """Normalize a value for comparison."""
    if val is None or pd.isna(val):
        return None
    
    if isinstance(val, str):
        normalized = val.strip().lower()
        if normalized in ['null', 'none', 'nan', '', 'n/a']:
            return None
        return normalized
    
    if isinstance(val, (int, float)):
        if isinstance(val, float):
            if val.is_integer():
                return int(val)
            return round(val, 6)
        return val
    
    return str(val).strip().lower()

def fuzzy_match_score(str1, str2):
    """Calculate fuzzy matching score between two strings."""
    if str1 == str2:
        return 1.0
    
    s1 = str1.lower().strip()
    s2 = str2.lower().strip()
    
    if s1 in s2 or s2 in s1:
        return 0.9
    
    return SequenceMatcher(None, s1, s2).ratio()

def detect_column_type(df, column_name, sample_size=100):
    """Detect the predominant data type of a column."""
    if column_name not in df.columns:
        return 'mixed'
    
    col_values = df[column_name].dropna()
    
    if len(col_values) == 0:
        return 'mixed'
    
    sample = col_values.head(min(sample_size, len(col_values)))
    
    numeric_count = 0
    string_count = 0
    
    for val in sample:
        if isinstance(val, (int, float, complex)):
            numeric_count += 1
        elif isinstance(val, str):
            try:
                float(val.strip())
                numeric_count += 1
            except (ValueError, AttributeError):
                string_count += 1
    
    total = numeric_count + string_count
    if total == 0:
        return 'mixed'
    
    if numeric_count / total >= 0.8:
        return 'numeric'
    elif string_count / total >= 0.8:
        return 'string'
    else:
        return 'mixed'

def are_types_compatible(type1, type2):
    """Check if two column types are compatible for mapping."""
    if type1 == 'mixed' or type2 == 'mixed':
        return True
    return type1 == type2

def map_columns_hybrid(gold_columns, test_columns, gold_df=None, test_df=None, fuzzy_threshold=0.7):
    """Map test columns to gold columns using type-aware hybrid approach."""
    mapping = {}
    used_gold_cols = set()
    used_test_cols = set()
    
    # Type classification
    gold_types = {}
    test_types = {}
    type_aware = False
    
    if gold_df is not None and test_df is not None:
        type_aware = True
        gold_types = {col: detect_column_type(gold_df, col) for col in gold_columns}
        test_types = {col: detect_column_type(test_df, col) for col in test_columns}
    
    # Stage 1: Fuzzy matching
    stage1_matches = 0
    for gold_col in gold_columns:
        best_match = None
        best_score = 0
        
        for test_col in test_columns:
            if test_col in used_test_cols:
                continue
            
            if type_aware:
                gold_type = gold_types.get(gold_col, 'mixed')
                test_type = test_types.get(test_col, 'mixed')
                if not are_types_compatible(gold_type, test_type):
                    continue
            
            score = fuzzy_match_score(gold_col, test_col)
            if score > best_score and score >= fuzzy_threshold:
                best_score = score
                best_match = test_col
        
        if best_match:
            mapping[best_match] = gold_col
            used_gold_cols.add(gold_col)
            used_test_cols.add(best_match)
            stage1_matches += 1
    
    # Stage 2: Positional fallback
    stage2_matches = 0
    unmapped_gold_cols = [col for col in gold_columns if col not in used_gold_cols]
    unmapped_test_cols = [col for col in test_columns if col not in used_test_cols]
    
    if type_aware:
        for gold_col in unmapped_gold_cols:
            gold_type = gold_types.get(gold_col, 'mixed')
            
            for test_col in unmapped_test_cols:
                if test_col in used_test_cols:
                    continue
                    
                test_type = test_types.get(test_col, 'mixed')
                
                if are_types_compatible(gold_type, test_type):
                    mapping[test_col] = gold_col
                    used_gold_cols.add(gold_col)
                    used_test_cols.add(test_col)
                    stage2_matches += 1
                    break
    else:
        num_to_pair = min(len(unmapped_gold_cols), len(unmapped_test_cols))
        for i in range(num_to_pair):
            test_col = unmapped_test_cols[i]
            gold_col = unmapped_gold_cols[i]
            mapping[test_col] = gold_col
            used_gold_cols.add(gold_col)
            used_test_cols.add(test_col)
            stage2_matches += 1
    
    final_unmapped_test = [col for col in test_columns if col not in used_test_cols]
    final_unmapped_gold = [col for col in gold_columns if col not in used_gold_cols]
    
    return mapping, final_unmapped_test, final_unmapped_gold, stage1_matches, stage2_matches

def normalize_dataframe(df):
    """Normalize all values in a dataframe."""
    df_normalized = df.copy()
    for col in df_normalized.columns:
        df_normalized[col] = df_normalized[col].apply(normalize_value)
    return df_normalized

def dataframe_to_row_set(df):
    """Convert dataframe rows to a set of hashable tuples."""
    row_set = set()
    for _, row in df.iterrows():
        row_tuple = tuple(row.values)
        row_set.add(row_tuple)
    return row_set


def dataframe_to_row_list(df):
    """Convert dataframe rows to an ordered list of hashable tuples."""
    return [tuple(row.values) for _, row in df.iterrows()]

def compare_dataframes(gold_df, test_df, order_sensitive=False):
    """Compare two dataframes with type-aware hybrid column mapping."""
    if len(gold_df) == 0 and len(test_df) == 0:
        return True
    
    if len(gold_df) == 0 and len(test_df) > 0:
        return False
    
    if len(test_df) == 0 and len(gold_df) > 0:
        return False
    
    # Normalize values
    gold_normalized = normalize_dataframe(gold_df)
    test_normalized = normalize_dataframe(test_df)
    
    # Map columns
    gold_columns = list(gold_normalized.columns)
    test_columns = list(test_normalized.columns)
    
    column_mapping, unmapped_test, unmapped_gold, stage1, stage2 = map_columns_hybrid(
        gold_columns, test_columns, gold_normalized, test_normalized
    )
    
    mapped_test_cols = [test_col for test_col in test_columns if test_col in column_mapping]
    
    if not mapped_test_cols:
        return False
    
    # Rename test columns
    test_renamed = test_normalized[mapped_test_cols].copy()
    test_renamed.columns = [column_mapping[col] for col in mapped_test_cols]
    
    # Keep only common columns (keeping as a double check against mapping logic)
    common_columns = [col for col in test_renamed.columns if col in gold_columns]
    
    if not common_columns:
        return False
    
    # Reorder dataframes
    gold_subset = gold_normalized[common_columns].copy()
    test_subset = test_renamed[common_columns].copy()
    
    if order_sensitive:
        gold_rows_list = dataframe_to_row_list(gold_subset)
        test_rows_list = dataframe_to_row_list(test_subset)

        # Keep multiplicity-aware diagnostics, but final pass requires same sequence.
        gold_counter = Counter(gold_rows_list)
        test_counter = Counter(test_rows_list)
        missing_rows = gold_counter - test_counter
        extra_rows = test_counter - gold_counter

        # ORDER BY queries must preserve row order exactly.
        is_success = gold_rows_list == test_rows_list
    else:
        # Order-insensitive comparison
        gold_rows = dataframe_to_row_set(gold_subset)
        test_rows = dataframe_to_row_set(test_subset)

        missing_rows = gold_rows - test_rows
        extra_rows = test_rows - gold_rows

        is_success = len(missing_rows) == 0 and len(extra_rows) == 0

        # Try row-as-set fallback
        if not is_success and len(gold_df) == len(test_df) and len(gold_df) > 0:
            gold_row_sets = set()
            for _, row in gold_subset.iterrows():
                gold_row_sets.add(frozenset(row.values))

            test_row_sets = set()
            for _, row in test_subset.iterrows():
                test_row_sets.add(frozenset(row.values))

            if gold_row_sets == test_row_sets:
                is_success = True
    
    return is_success

def load_csv_safely(file_path):
    """Safely load a CSV file."""
    try:
        if not os.path.exists(file_path):
            return None
        
        if os.path.getsize(file_path) == 0:
            return pd.DataFrame()
        
        df = pd.read_csv(file_path)
        return df
    except:
        return None

def find_matching_sqlite_file(query_num, sqlite_dir):
    """Find the matching sqlite file for a given query number."""
    pattern = f"question_{query_num}_*.csv"
    matches = list(sqlite_dir.glob(pattern))
    if matches:
        return matches[0]
    return None

def run_comparison(csv_output_dir: Path, sqlite_dir: Path, order_sensitive_query_ids: Optional[Set[str]] = None) -> Set[int]:
    """Run comparison and return set of failed query IDs."""
    test_files = sorted(csv_output_dir.glob("*.csv"))
    failed_ids = set()
    order_sensitive_query_ids = order_sensitive_query_ids or set()
    
    for test_file in test_files:
        query_num = test_file.stem  # Get filename without extension
        
        gold_file = find_matching_sqlite_file(query_num, sqlite_dir)
        if not gold_file:
            failed_ids.add(int(query_num))
            continue
        
        gold_df = load_csv_safely(gold_file)
        test_df = load_csv_safely(test_file)
        
        if gold_df is None or test_df is None:
            failed_ids.add(int(query_num))
            continue
        
        is_order_sensitive = query_num in order_sensitive_query_ids
        is_success = compare_dataframes(gold_df, test_df, order_sensitive=is_order_sensitive)
        
        if not is_success:
            failed_ids.add(int(query_num))
    
    return failed_ids

# ============================================================================
# MAIN PIPELINE ORCHESTRATION
# ============================================================================

def run_testiq(questions: List[Dict], prompt_template: str, output_baseline: str, config: Dict):
    """Run test IQ logic on a set of questions."""
    output_dir = SCRIPT_DIR / "couchbase_iq_results" / output_baseline
    output_sql_dir = output_dir / "sql_outputs"
    output_csv_dir = output_dir / "csv_outputs"
    
    output_dir.mkdir(exist_ok=True, parents=True)
    output_sql_dir.mkdir(exist_ok=True)
    output_csv_dir.mkdir(exist_ok=True)
    
    couchbase_config = config.get("couchbase", {})
    couchbase_iq_config = config.get("couchbase_iq", {})
    
    couchbase_user = couchbase_config.get("username", "Administrator")
    couchbase_pass = couchbase_config.get("password", "password")
    query_endpoint = resolve_query_endpoint_from_config(couchbase_config, couchbase_user, couchbase_pass)
    natural_cred = couchbase_iq_config.get("natural_cred", "<capella_username:password>")
    natural_orgid = couchbase_iq_config.get("natural_orgid", "<capella_orgid>")

    log_print(f"Using Query endpoint: {query_endpoint}")
    
    couchbase_iq = CouchbaseIqAPI(
        query_endpoint=query_endpoint,
        natural_cred=natural_cred,
        natural_orgid=natural_orgid,
        couchbase_user=couchbase_user,
        couchbase_pass=couchbase_pass,
        prompt_template=prompt_template
    )
    
    stats = {'total': 0, 'success': 0, 'failed': 0, 'skipped': 0}
    
    for i, item in enumerate(questions, 1):
        qId = item['question_id']
        question = item['question']
        db_id = item['db_id']
        
        stats['total'] += 1
        
        log_print(f"\n[{i}/{len(questions)}] {qId}")
        log_print(f"  Question: {question[:80]}...")
        log_print(f"  Database: {db_id}")
        
        # Check if outputs already exist
        sql_file = output_sql_dir / f"{qId}.sql"
        csv_file = output_csv_dir / f"{qId}.csv"
        
        if sql_file.exists() and csv_file.exists():
            log_print(f"  ⊙ Skipping (already processed)")
            stats['skipped'] += 1
            continue
        
        try:
            success = process_question(
                item=item,
                couchbase_iq=couchbase_iq,
                couchbase_user=couchbase_user,
                couchbase_pass=couchbase_pass,
                query_endpoint=query_endpoint,
                output_sql_dir=output_sql_dir,
                output_csv_dir=output_csv_dir
            )
            
            if success:
                stats['success'] += 1
                log_print("  ✓ Success")
            else:
                stats['failed'] += 1
                log_print("  ✗ Failed")
        except Exception as e:
            stats['failed'] += 1
            log_print(f"  ✗ Exception: {e}")
        
        time.sleep(0.5)
    
    return stats, output_csv_dir

def main():
    """Main pipeline execution."""
    log_print("="*80)
    log_print("SQL GENERATION AND VALIDATION PIPELINE")
    log_print("="*80)
    
    write_pipeline_header()
    
    # Load configuration
    config = load_config()
    
    # Load all questions
    all_questions = load_questions()
    order_sensitive_query_ids = load_order_sensitive_query_ids()
    total_questions = len(all_questions)
    log_print(f"\nTotal questions in dataset: {total_questions}")
    log_print(f"ORDER BY queries (order-sensitive comparison): {len(order_sensitive_query_ids)}")
    
    # ========================================================================
    # STAGE 1: Initial run with INITIAL_PROMPT
    # ========================================================================
    log_print("\n" + "="*80)
    log_print("STAGE 1: Initial Test Run")
    log_print("="*80)
    
    initial_stats, initial_csv_dir = run_testiq(
        questions=all_questions,
        prompt_template=INITIAL_PROMPT,
        output_baseline=INITIAL_RUN_BASELINE,
        config=config
    )
    
    log_print(f"\nInitial run complete:")
    log_print(f"  Total: {initial_stats['total']}")
    log_print(f"  Success: {initial_stats['success']}")
    log_print(f"  Failed: {initial_stats['failed']}")
    log_print(f"  Skipped: {initial_stats['skipped']}")
    
    # ========================================================================
    # STAGE 2: Compare results and identify failures
    # ========================================================================
    log_print("\n" + "="*80)
    log_print("STAGE 2: Compare Results")
    log_print("="*80)
    
    log_print("\nRunning comparison on all CSV files in baseline...")
    failed_ids = run_comparison(initial_csv_dir, SQLITE_RESULTS_DIR, order_sensitive_query_ids)
    
    # Count how many files were compared
    csv_files_compared = len(list(initial_csv_dir.glob("*.csv")))
    
    log_print(f"\nComparison complete:")
    log_print(f"  Files compared: {csv_files_compared}")
    log_print(f"  Successful queries: {csv_files_compared - len(failed_ids)}")
    log_print(f"  Failed queries: {len(failed_ids)}")
    
    if not failed_ids:
        log_print("\n" + "="*80)
        log_print("🎉 No failed queries! All tests passed.")
        log_print("="*80)
        
        # Calculate and display final stats based on comparison results
        final_success_percentage = (csv_files_compared / csv_files_compared * 100) if csv_files_compared > 0 else 0
        log_print(f"\nFINAL SUCCESS RATE: {csv_files_compared}/{csv_files_compared} = {final_success_percentage:.2f}%")
        return
    
    log_print(f"\nFailed query IDs: {sorted(list(failed_ids))}")
    
    # ========================================================================
    # STAGE 3: Retry failed queries with RETRY_PROMPT
    # ========================================================================
    log_print("\n" + "="*80)
    log_print("STAGE 3: Retry Failed Queries")
    log_print("="*80)
    
    # Filter questions to only failed IDs
    failed_questions = [q for q in all_questions if q.get('question_id') in failed_ids]
    log_print(f"\nRetrying {len(failed_questions)} failed queries...")
    
    retry_stats, retry_csv_dir = run_testiq(
        questions=failed_questions,
        prompt_template=RETRY_PROMPT,
        output_baseline=RETRY_RUN_BASELINE,
        config=config
    )
    
    log_print(f"\nRetry run complete:")
    log_print(f"  Total: {retry_stats['total']}")
    log_print(f"  Success: {retry_stats['success']}")
    log_print(f"  Failed: {retry_stats['failed']}")
    log_print(f"  Skipped: {retry_stats['skipped']}")
    
    # ========================================================================
    # STAGE 4: Compare retry results
    # ========================================================================
    log_print("\n" + "="*80)
    log_print("STAGE 4: Compare Retry Results")
    log_print("="*80)
    
    log_print("\nRunning comparison on retry output files...")
    still_failed_ids = run_comparison(retry_csv_dir, SQLITE_RESULTS_DIR, order_sensitive_query_ids)
    
    # Count how many retry files were compared
    retry_csv_files_compared = len(list(retry_csv_dir.glob("*.csv")))
    
    log_print(f"\nRetry comparison complete:")
    log_print(f"  Retry files compared: {retry_csv_files_compared}")
    log_print(f"  Successful queries: {retry_csv_files_compared - len(still_failed_ids)}")
    log_print(f"  Still failing: {len(still_failed_ids)}")
    
    # ========================================================================
    # FINAL SUMMARY
    # ========================================================================
    log_print("\n" + "="*80)
    log_print("PIPELINE SUMMARY")
    log_print("="*80)
    
    # Final result combines: initial successes + queries that succeeded on retry
    initial_successes = csv_files_compared - len(failed_ids)
    retry_successes = len(failed_ids - still_failed_ids)
    total_successes = initial_successes + retry_successes
    final_success_percentage = (total_successes / total_questions * 100) if total_questions > 0 else 0
    
    log_print(f"Total questions: {total_questions}")
    log_print(f"Initial run successes: {initial_successes}")
    log_print(f"Initial run failures: {len(failed_ids)}")
    log_print(f"Retry run successes: {retry_successes}")
    log_print(f"Still failing after retry: {len(still_failed_ids)}")
    log_print(f"\n{'='*80}")
    log_print(f"FINAL SUCCESS RATE: {total_successes}/{total_questions} = {final_success_percentage:.2f}%")
    log_print(f"{'='*80}")
    
    if still_failed_ids:
        log_print(f"\nStill-failed query IDs: {sorted(list(still_failed_ids))}")
    else:
        log_print("\n🎉 All previously failed queries now pass!")
    
    log_print("\n" + "="*80)
    log_print(f"Pipeline completed at: {get_ist_timestamp()}")
    log_print(f"Full log saved to: {PIPELINE_LOG}")
    log_print("="*80)

if __name__ == "__main__":
    main()
