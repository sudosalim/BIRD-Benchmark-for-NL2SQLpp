#!/usr/bin/env python3
"""
Test IQ for specific question IDs.
Usage: 
    python testIQ_single.py 89 92 95
    or specify IDs in QUESTION_IDS list below
"""
import json
from pathlib import Path
import requests
from typing import List, Dict, Optional
import time
import csv
import re
from datetime import datetime, timedelta
import sys
from couchbase_endpoint_utils import resolve_query_endpoint_from_config

SCRIPT_DIR = Path(__file__).parent
CONFIG_FILE = SCRIPT_DIR.parent / "IQ_config.json"
MANUAL_KEYSPACES_FILE = SCRIPT_DIR / "manual_keyspaces.json"
OUTPUT_DIR = SCRIPT_DIR / "couchbase_iq_results"
OUTPUT_SQL_DIR = OUTPUT_DIR / "sql_outputs"
OUTPUT_CSV_DIR = OUTPUT_DIR / "csv_outputs"
JSON_OUTPUT_DIR = SCRIPT_DIR / "sqlLiteToJson" / "json_outputs"

# Specify question IDs here if not passing via command line
# Example: QUESTION_IDS = [89, 92, 95, 115]
QUESTION_IDS = []


# Default Configuration (overridden by couchbase_config.json if exists)
DEFAULT_CONFIG = {
    "couchbase": {
        "cb_endpoint": "127.0.0.1:9000",
        "username": "Administrator",
        "password": "password"
    },
    "couchbase_iq": {
        "natural_cred": "<capella_username:password>",
        "natural_orgid": "<capella_orgid>"
    },
    "evaluation": {
        "max_questions": None,
        "save_sql": True,
        "execute_sql": True,
        "delay_between_requests": 0.5,
        "rerun_existing": False
    }
}

class CouchbaseIqAPI:
    def __init__(self, query_endpoint, natural_cred, natural_orgid, couchbase_user, couchbase_pass):
        self.query_endpoint = query_endpoint
        self.natural_cred = natural_cred
        self.natural_orgid = natural_orgid
        self.couchbase_user = couchbase_user
        self.couchbase_pass = couchbase_pass
        
        # Error code -> additional statement text mapping
        # Add text to append to statement when specific error codes are encountered
        self.error_code_statement_additions = {
            19212: "I know that the required information exists in the provided keyspaces and fields. Please try harder and generate a correct SQL++ query using only the provided keyspaces and fields. Remember to follow all the constraints mentioned earlier and return only what is asked in the question.",
            19214 : "Don't use Corelated subqueries. All subqueries must be non-correlated. A subquery must not reference outer query aliases or columns. Each query block must use only its own locally defined aliases."
        }

    def generate_sql(self, keyspaces: List[str], question: str, evidence: Optional[str] = None, max_retries: int = 10) -> tuple:
        """
        Generate SQL using Couchbase IQ with retry logic for error code 19214.
        
        Args:
            keyspaces: List of keyspace/database names
            question: Natural language question
            evidence: Optional additional context/evidence
            max_retries: Maximum number of attempts (default: 4)
            
        Returns:
            Tuple of (generated_sql, statement) where statement is the full request sent to IQ
        """
        print(f"================================================= SS ================================================")
        with_params = {
            "keyspaces": keyspaces,
            "execute": False
        }
 
        # Build the statement with the WITH clause
        with_clause_json = json.dumps(with_params)
        statement = f"USING AI WITH {with_clause_json}. Generate a syntactically correct query that MUST follow ALL rules: use only provided keyspaces and its and only its existing fields, do not create your own fields; never use SELECT *; the main SELECT must NOT use RAW and must return only standard expressions; RAW is allowed only inside SELECT subqueries; scalar subquery access must use IN (SELECT RAW ...) or = (SELECT RAW ...)[0]; any SELECT RAW used as a scalar MUST be immediately followed by [0]; NEVER apply [0] to MAX(), MIN(), AVG(), or other aggregate functions; do NOT use correlated subqueries (use JOINs only); do not repeat aliases and never name an alias the same as a bucket/table; do not JOIN tables unless required by the question and never self-JOIN unless needed; Use only functions supported by Couchbase SQL++ for example their is NO DIVIDE(),SUBTRACT() in SQL++; use DISTINCT when unique entities are requested; if an object is requested without specific attributes, return its id; do not add ':' after FROM; every field in the final SELECT or WHERE clause MUST have an explicit IS NOT NULL condition in WHERE unless that field is explicitly checked for IS NULL in SELECT logic; PERCENTAGE RULE (MANDATORY): numerator and denominator MUST be computed from the SAME row set; computing the denominator using a separate subquery is FORBIDDEN; define a single base set (via FROM/JOIN/WHERE) representing the full population, compute the denominator from this base set, and derive the numerator ONLY by adding conditions inside aggregate expressions (e.g., CASE WHEN); GROUP BY should not be used when only one keyspace is involved; violating this rule is NOT allowed; return ONLY what is asked. Q: {question} Hint: {evidence}"
        
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

        # Track which error codes have been handled (to allow multiple different error codes to add text)
        handled_error_codes = set()
        
        # Retry loop
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
                if response is not None:
                    try:
                        error_data = response.json()
                        # Extract error code if available
                        if "errors" in error_data and len(error_data["errors"]) > 0:
                            error_code = error_data["errors"][0].get("code")
                            error_msg = error_data["errors"][0].get("msg", "")
                            print(f"  ✗ Error generating SQL - Code: {error_code} (Attempt {attempt}/{max_retries})")
                            print(f"     Message: {error_msg[:200]}...")  # First 200 chars
                            
                            # Check if this error code has a statement modification and hasn't been handled yet
                            if error_code in self.error_code_statement_additions and error_code not in handled_error_codes:
                                additional_text = self.error_code_statement_additions[error_code]
                                statement = statement + additional_text
                                payload["statement"] = statement
                                handled_error_codes.add(error_code)
                                print(f"  ⚡ Error {error_code} detected - modifying statement with additional instructions")
                                print(f"  Added text: {additional_text}")
                                print(f"  ↻ Retrying with modified statement...")
                                time.sleep(1)
                                continue
                            
                            # Special handling for error 19214 - ADVISE logic
                            if error_code == 19214 and error_code in handled_error_codes and f"{error_code}_ADVISE_ATTEMPTED" not in handled_error_codes:
                                # If we've already tried the prompt addition, try ADVISE approach
                                print(f"  ⚡ Error 19214 persists - attempting ADVISE approach...")
                                print(f"=== DEBUG: ERROR MESSAGE ===")
                                print(f"Type: {type(error_msg)}")
                                print(f"Length: {len(error_msg)}")
                                print(f"First 200 chars: {repr(error_msg[:200])}")
                                print(f"Contains ``` : {'```' in error_msg}")
                                print(f"=== END DEBUG ===\n")
                                
                                # Extract SQL from between ``` markers
                                # Try multiple patterns
                                sql_match = None
                                extracted_sql = None
                                
                                try:
                                    # Pattern 1: Standard markdown code block
                                    sql_match = re.search(r'```\s*(.*?)\s*```', error_msg, re.DOTALL)
                                    if sql_match:
                                        print(f"✓ Matched with Pattern 1 (standard markdown)")
                                        extracted_sql = sql_match.group(1).strip()
                                    
                                    # Pattern 2: Code block without closing backticks
                                    if not extracted_sql:
                                        sql_match = re.search(r'```\s*\n?(.*?)$', error_msg, re.DOTALL)
                                        if sql_match:
                                            print(f"✓ Matched with Pattern 2 (no closing backticks)")
                                            extracted_sql = sql_match.group(1).strip()
                                    
                                    # Pattern 3: After "statement- ```"
                                    if not extracted_sql:
                                        sql_match = re.search(r'statement-\s*```\s*\n?(.*?)(?:\n```|$)', error_msg, re.DOTALL)
                                        if sql_match:
                                            print(f"✓ Matched with Pattern 3 (after 'statement-')")
                                            extracted_sql = sql_match.group(1).strip()
                                except Exception as pattern_error:
                                    print(f"  ✗ Error during pattern matching: {pattern_error}")
                                    import traceback
                                    traceback.print_exc()
                                
                                if extracted_sql:
                                    print(f"  ✓ Extracted SQL from error message ({len(extracted_sql)} chars)")
                                    print(f"  📝 EXTRACTED SQL:\n{extracted_sql}\n")
                                    
                                    # Run ADVISE query
                                    advise_sql = f"ADVISE {extracted_sql}"
                                    print(f"  🔍 Running ADVISE query...")
                                    
                                    advise_results = self._execute_query(advise_sql)
                                    print(f"  📊 ADVISE Results status: {advise_results.get('status') if advise_results else 'None'}")
                                
                                    if advise_results and "results" in advise_results and advise_results["results"]:
                                        advice_data = advise_results["results"][0]
                                        if "advice" in advice_data:
                                            # Execute recommended CREATE INDEX statements and track created indexes
                                            index_success, created_indexes = self._execute_index_recommendations(advice_data["advice"])
                                            if index_success:
                                                # Try the original query again
                                                print(f"  🔄 Retrying original query after index creation...")
                                                final_results = self._execute_query(extracted_sql)
                                                
                                                # Cleanup: Delete the created indexes
                                                self._delete_indexes(created_indexes)
                                                
                                                if final_results and final_results.get("status") == "success":
                                                    print(f"  ✅ Query succeeded after index optimization!")
                                                    print(f"================================================= SS ================================================")
                                                    return (extracted_sql, statement)
                                                else:
                                                    print(f"  ⚠️ Query still failed after index optimization")
                                            else:
                                                print(f"  ⚠️ No indexes were created successfully")
                                        else:
                                            print(f" No advice found in ADVISE results")
                                    else:
                                        print(f" ADVISE query failed or returned no results")
                                else:
                                    print(f"  ✗ Could not extract SQL from error message")
                                
                                # Mark that ADVISE was attempted to avoid infinite loops
                                handled_error_codes.add(f"{error_code}_ADVISE_ATTEMPTED")
                                
                                # Continue with normal retry logic
                            
                            # For error codes 19214 or 19212, extract SQL from between ``` markers as fallback
                            # Only attempt extraction after 5 attempts
                            if error_code in [19214, 19212] and attempt >= 5:
                                print(f"  ⚡ Error {error_code} detected (attempt {attempt}) - extracting SQL from error message...")
                                # Extract content between ``` markers
                                sql_match = re.search(r'```\s*(.*?)\s*```', error_msg, re.DOTALL)
                                if sql_match:
                                    extracted_sql = sql_match.group(1).strip()
                                    print(f"  ✓ Extracted SQL from error message ({len(extracted_sql)} chars)")
                                    print(f"  Extracted SQL: {extracted_sql}")
                                    print(f"================================================= SS ================================================")
                                    return (extracted_sql, statement)
                                else:
                                    print(f"  ✗ Could not extract SQL from error message")
                            
                            # Continue retrying if we haven't reached max attempts
                            if attempt < max_retries:
                                print(f"  ↻ Retrying request...")
                                time.sleep(1)
                                continue
                        else:
                            print(f"  ✗ Error generating SQL: {error_data}")
                    except:
                        print(f"  ✗ Error generating SQL: {response.text}")
                
                    print(f"  ✗ Error generating SQL: {e}")
                
                # If we reach here, don't retry (non-19214 error or max retries reached)
                print(f"================================================= SS ================================================")
                return ("", statement)
    
    def _execute_query(self, sql_query: str) -> Optional[Dict]:
        """
        Execute a SQL query using the same endpoint and credentials.
        
        Args:
            sql_query: SQL query to execute
            
        Returns:
            Query response as dictionary, or None if error
        """
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
            # Don't raise for status immediately, first check the response
            if response.status_code != 200:
                print(f"  ⚠️ Server returned {response.status_code}: {response.text[:500]}...")
                return None
            
            data = response.json()
            # Check if the response contains errors
            if data.get("status") == "fatal" or "errors" in data:
                print(f"  ⚠️ Query failed with errors: {data}")
                return None
                
            return data
            
        except requests.exceptions.RequestException as e:
            print(f"  ✗ Error executing query (RequestException): {e}")
            return None
        except Exception as e:
            print(f"  ✗ Error executing query (General): {e}")
            return None
    
    def _execute_index_recommendations(self, advice_data: Dict) -> tuple:
        """
        Execute CREATE INDEX statements from ADVISE recommendations.
        
        Args:
            advice_data: The advice section from ADVISE query results
            
        Returns:
            Tuple of (success: bool, created_index_names: list)
        """
        if not advice_data or "adviseinfo" not in advice_data:
            print(f" No advise info found")
            return (False, [])
        
        advise_info = advice_data["adviseinfo"]
        if "recommended_indexes" not in advise_info:
            print(f" No recommended indexes found")
            return (False, [])
        
        recommended = advise_info["recommended_indexes"]
        created_indexes = []  # List of (index_name, keyspace) tuples
        
        # Try covering indexes first
        if "covering_indexes" in recommended:
            print(f" Creating {len(recommended['covering_indexes'])} covering indexes...")
            for idx in recommended["covering_indexes"]:
                if "index_statement" in idx:
                    print(f"  🔨 Creating: {idx['index_statement']}")
                    result = self._execute_query(idx["index_statement"])
                    if result and result.get("status") == "success":
                        print(f" Index created successfully")
                        # Extract index name and keyspace from CREATE INDEX statement
                        index_info = self._extract_index_name(idx["index_statement"])
                        if index_info:
                            created_indexes.append(index_info)
                    else:
                        print(f" Index creation failed or already exists")
        
        # Try regular indexes
        if "indexes" in recommended:
            print(f" Creating {len(recommended['indexes'])} regular indexes...")
            for idx in recommended["indexes"]:
                if "index_statement" in idx:
                    print(f"  🔨 Creating: {idx['index_statement']}")
                    result = self._execute_query(idx["index_statement"])
                    if result and result.get("status") == "success":
                        print(f" Index created successfully")
                        # Extract index name and keyspace from CREATE INDEX statement
                        index_info = self._extract_index_name(idx["index_statement"])
                        if index_info:
                            created_indexes.append(index_info)
                    else:
                        print(f" Index creation failed or already exists")
        
        print(f" Successfully created {len(created_indexes)} indexes")
        return (len(created_indexes) > 0, created_indexes)
    
    def _extract_index_name(self, create_statement: str) -> Optional[tuple]:
        """
        Extract index name and keyspace from CREATE INDEX statement.
        
        Args:
            create_statement: CREATE INDEX SQL statement
            
        Returns:
            Tuple of (index_name, keyspace) or None if not found
        """
        # Pattern: CREATE INDEX index_name ON keyspace(...)
        # Keyspace can be: `default`:`bucket`.`scope`.`collection` or `bucket`.`scope`.`collection`
        match = re.search(r'CREATE\s+INDEX\s+(`?[\w]+`?)\s+ON\s+(`[^`]+`(?::`[^`]+`)*(?:\.`[^`]+`)*)\s*\(', create_statement, re.IGNORECASE)
        if match:
            index_name = match.group(1).strip('`')
            keyspace = match.group(2)  # Keep backticks in keyspace
            return (index_name, keyspace)
        return None
    
    def _delete_indexes(self, index_info_list: List[tuple]) -> None:
        """
        Delete a list of indexes.
        
        Args:
            index_info_list: List of (index_name, keyspace) tuples
        """
        if not index_info_list:
            return
        
        print(f"  🧹 Cleaning up {len(index_info_list)} created indexes...")
        for index_name, keyspace in index_info_list:
            # keyspace already has backticks, index_name needs them
            drop_statement = f"DROP INDEX `{index_name}` ON {keyspace}"
            print(f"  🗑️  Dropping: {drop_statement}")
            result = self._execute_query(drop_statement)
            if result and result.get("status") == "success":
                print(f"  ✓ Index dropped successfully")
            else:
                print(f"  ⚠️ Index drop failed (may not exist)")
        

def load_manual_keyspaces() -> Dict:
    """
    Load manual keyspace mappings from JSON file.
    
    Returns:
        Dictionary mapping question_id (as string) to list of collection names
    """
    if not MANUAL_KEYSPACES_FILE.exists():
        return {}
    
    try:
        with open(MANUAL_KEYSPACES_FILE, 'r') as f:
            data = json.load(f)
        
        # Remove special keys starting with underscore (comments/examples)
        manual_mappings = {k: v for k, v in data.items() if not k.startswith('_')}
        return manual_mappings
    except Exception as e:
        print(f"Warning: Failed to load manual keyspaces file: {e}")
        return {}


def get_all_collections_in_scope(
    bucket_name: str,
    scope_name: str,
):
    """
    Get all collections in a scope along with their field/column names.
    
    Args:
        bucket_name: Name of the bucket (database)
        scope_name: Name of the scope
        
    Returns:
        Dictionary with collection names as keys and list of field names as values
        Example: {
            'students': ['id', 'name', 'age', 'grade'],
            'courses': ['id', 'title', 'credits']
        }
    """
    json_path = Path(JSON_OUTPUT_DIR) / f"{bucket_name}.json"

    if not json_path.exists():
        print(f"Error: File not found: {json_path}")
        return {}

    # Load the JSON file
    print("Loading JSON file...")
    try:
        with open(json_path, 'r') as f:
            data = json.load(f)
    except json.JSONDecodeError as e:
        print(f"Error: Invalid JSON format: {e}")
        return {}
    except Exception as e:
        print(f"Error reading file: {e}")
        return {}
    
    # Build dictionary of collections with their fields
    collections_with_fields = {}
    
    for table_name, table_data in data.items():
        # Skip sqlite internal tables
        if table_name == "sqlite_sequence":
            continue
        
        # Extract field names
        fields = []
        # print(table_name)
        if isinstance(table_data, list) and len(table_data) > 0 and isinstance(table_data[0], dict):
            fields = list(table_data[0].keys())
        
        collections_with_fields[table_name] = fields
    
    return collections_with_fields


def exec_sql(sql_query: str, couchbase_user: str, couchbase_pass: str, query_endpoint: str) -> Optional[List[Dict]]:
    """
    Execute SQL query on Couchbase and return results.
    
    Args:
        sql_query: SQL query to execute
        couchbase_user: Couchbase username
        couchbase_pass: Couchbase password
        query_endpoint: Query service endpoint
        
    Returns:
        List of result rows as dictionaries, or None if error
    """
    # Prepare request
    payload = {
        "statement": sql_query,
        "timeout": "300s"
    }
    
    headers = {
        "Content-Type": "application/json"
    }

    # Send request
    try:
        response = requests.post(
            query_endpoint,
            json=payload,
            headers=headers,
            auth=(couchbase_user, couchbase_pass)
        )
        response.raise_for_status()
        data = response.json()
        
        # Extract results
        if "results" in data:
            return data["results"]
        else:
            print(f"  Warning: No 'results' field in response")
            return []
            
    except requests.exceptions.RequestException as e:
        print(f"  Error executing SQL: {e}")
        if hasattr(e, 'response') and e.response is not None:
            try:
                error_data = e.response.json()
                print(f"  Error details: {error_data}")
            except:
                print(f"  Response text: {e.response.text}")
        return None


def json_to_csv(data: List[Dict], output_file: Path) -> bool:
    """
    Convert JSON results to CSV file.
    
    Args:
        data: List of dictionaries (query results)
        output_file: Path to output CSV file
        
    Returns:
        True if successful, False otherwise
    """
    if not data:
        print(f"  Warning: No data to write to CSV")
        # Create empty file
        output_file.touch()
        return True
    
    try:
        # Get all unique keys from all rows while preserving order from first row
        # Use dict.fromkeys() to maintain insertion order (Python 3.7+)
        fieldnames = {}
        for row in data:
            for key in row.keys():
                fieldnames[key] = None  # Using dict to preserve order
        fieldnames = list(fieldnames.keys())  # Preserves original query order
        
        with open(output_file, 'w', newline='') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(data)
        
        return True
    except Exception as e:
        print(f"  Error writing CSV: {e}")
        return False


def load_config() -> Dict:
    """Load configuration from JSON file or use defaults."""
    if CONFIG_FILE.exists():
        try:
            with open(CONFIG_FILE, 'r') as f:
                return json.load(f)
        except Exception as e:
            print(f"Warning: Failed to load config file: {e}")
            print("Using default configuration")
    return DEFAULT_CONFIG


def loadQuestions(question_ids: List[int] = None) -> List[Dict]:
    """
    Load questions from mini_dev_sqlite.json, optionally filtering by question IDs.
    
    Args:
        question_ids: List of question IDs to load (loads all if None)
    
    Returns:
        List of question dictionaries
    """
    json_file = SCRIPT_DIR.parent / "mini_dev_data" / "mini_dev_sqlite.json"
    
    if not json_file.exists():
        raise FileNotFoundError(f"Questions file not found: {json_file}")
    
    with open(json_file, 'r') as f:
        all_questions = json.load(f)
    
    # Filter by question IDs if provided
    if question_ids:
        question_id_set = set(question_ids)
        filtered_questions = [q for q in all_questions if q.get('question_id') in question_id_set]
        return filtered_questions
    
    return all_questions


def reduce_keyspace_via_collection(statements: List[str], collections_dict: Dict[str, List[str]], max_keyspaces: int = 4) -> List[str]:
    """
    Select most relevant collections based on question/evidence text and schema.
    Returns top N collections ranked by relevance score.
    
    Args:
        statements: List of strings to match against (e.g., [question, evidence])
        collections_dict: Dictionary of {collection_name: [field_names]}
        max_keyspaces: Maximum number of keyspaces to return (default: 4)
        
    Returns:
        List of most relevant collection names (up to max_keyspaces)
    """
    # Combine all statements into one text
    combined_text = " ".join(s.lower() if s else "" for s in statements)
    
    # Extract all words from the combined text
    text_words = set(re.findall(r"\b\w+\b", combined_text))
    
    # Score each keyspace
    keyspace_scores = {}
    
    for collection_name, fields in collections_dict.items():
        collection_lower = collection_name.lower()
        score = 0
        
        # === COLLECTION NAME SCORING ===
        
        # Exact word match
        if collection_lower in text_words:
            score += 10
        
        # Collection name is substring of any word in text
        for word in text_words:
            if len(word) >= 4 and (collection_lower in word or word in collection_lower):
                score += 5
                break
        
        # Handle underscores - split and match parts
        collection_parts = collection_lower.replace('_', ' ').split()
        for part in collection_parts:
            if len(part) >= 3:
                if part in text_words:
                    score += 3
                # Partial match
                for word in text_words:
                    if len(word) >= 4 and (part in word or word in part):
                        score += 1
        
        # Handle plurals
        for word in text_words:
            if len(word) > 3:
                singular = word.rstrip('s')
                if singular in collection_lower or collection_lower in singular:
                    score += 2
        
        # === FIELD/COLUMN NAME SCORING (HIGHER WEIGHT) ===
        
        for field in fields:
            field_lower = field.lower()
            
            # Exact field match (highest score!)
            if field_lower in text_words:
                score += 15
            
            # Field substring match
            for word in text_words:
                if len(word) >= 4 and (field_lower in word or word in field_lower):
                    score += 7
            
            # Handle underscore in field names
            field_parts = field_lower.replace('_', ' ').split()
            for part in field_parts:
                if len(part) >= 3 and part in text_words:
                    score += 5
            
            # Field plural handling
            for word in text_words:
                if len(word) > 3:
                    singular = word.rstrip('s')
                    if singular in field_lower or field_lower in singular:
                        score += 4
        
        # Store score if any relevance found
        if score > 0:
            keyspace_scores[collection_name] = score
    
    # If no matches found, return top collections by field count (likely central tables)
    if not keyspace_scores:
        sorted_by_fields = sorted(collections_dict.items(), key=lambda x: len(x[1]), reverse=True)
        return [name for name, _ in sorted_by_fields[:max_keyspaces]]
    
    # Sort by score (descending), then by field count (more fields = more central)
    sorted_keyspaces = sorted(
        keyspace_scores.items(),
        key=lambda x: (x[1], len(collections_dict.get(x[0], []))),
        reverse=True
    )
    
    # Return top N keyspaces
    top_keyspaces = [k for k, _ in sorted_keyspaces[:max_keyspaces]]
    
    return top_keyspaces


def get_keyspaces_for_db(db_id: str, qId: int, question: str, evidence: Optional[str] = None) -> List[str]:
    """
    Get keyspace names for a given database.
    Returns the bucket.scope format for Couchbase.
    
    Args:
        db_id: Database identifier
        qId: Question ID
        question: Natural language question
        evidence: Additional context/evidence
        
    Returns:
        List of all keyspace names from the database JSON file
    """
    # For BIRD mini-dev, we use bucket name as db_id with bird scope
    # Format: bucket.scope.collection

    # Get all collections from the database JSON file
    collections_dict = get_all_collections_in_scope(
        bucket_name=db_id,
        scope_name="bird",
    )
    
    # Use ALL collections without filtering
    selected_collections = list(collections_dict.keys())
    
    # Format as keyspaces
    keyspaces = []
    for coll in selected_collections:
        keyspaces.append(f"`{db_id}`.`bird`.`{coll}`")
    
    return keyspaces


def write_run_header(failed_file: Path):
    """Write a run header to the failed questions log."""
    ist_tz = timedelta(hours=5, minutes=30)
    now = datetime.utcnow() + ist_tz
    timestamp = now.strftime("%Y-%m-%d %H:%M:%S IST")
    
    with open(failed_file, 'a') as f:
        f.write("\n")
        f.write("=" * 80 + "\n")
        f.write(json.dumps({
            "type": "RUN_START",
            "timestamp": timestamp,
            "message": "New run started"
        }) + "\n")
        f.write("=" * 80 + "\n")


def log_failure_immediately(failed_file: Path, question_id: int, question: str, db_id: str, reason: str, statement: str = None):
    """Log a failed question immediately to the failed questions file."""
    ist_tz = timedelta(hours=5, minutes=30)
    now = datetime.utcnow() + ist_tz
    timestamp = now.strftime("%Y-%m-%d %H:%M:%S IST")
    
    failure_record = {
        "timestamp": timestamp,
        "question_id": question_id,
        "question": question,
        "db_id": db_id,
        "reason": reason
    }
    
    # Add statement field if provided
    if statement:
        failure_record["statement"] = statement
    
    with open(failed_file, 'a') as f:
        f.write(json.dumps(failure_record) + "\n")


def process_question(
    item: Dict,
    couchbase_iq: CouchbaseIqAPI,
    couchbase_user: str,
    couchbase_pass: str,
    query_endpoint: str,
    manual_keyspaces_map: Dict = None,
    save_sql: bool = True,
    execute_sql: bool = True
) -> tuple:
    """
        Process a single question: generate SQL, execute, and save results.
        
        Args:
            item: Question item from spider2-lite.jsonl
            couchbase_iq: CouchbaseIQ API instance
            manual_keyspaces_map: Manual keyspace mappings (question_id -> collection names)
            save_sql: Whether to save generated SQL
            execute_sql: Whether to execute SQL and save CSV
            
        Returns:
            Tuple of (success, sql_generated, csv_saved)
    """

    qId=item['question_id']
    question=item['question']
    evidence=item.get('evidence',None)
    db_id=item['db_id']

    # Check manual keyspace mappings first
    if manual_keyspaces_map and str(qId) in manual_keyspaces_map:
        collection_names = manual_keyspaces_map[str(qId)]
        keyspaces = [f"`{db_id}`.`bird`.`{col}`" for col in collection_names]
        print(f"  Using manual keyspaces: {keyspaces}")
    else:
        keyspaces=get_keyspaces_for_db(db_id,qId,question,evidence)

    print(f"  Generating SQL with Couchbase IQ...")
    generated_sql, statement = couchbase_iq.generate_sql(
        keyspaces=keyspaces,
        question=question,
        evidence=evidence
    )
    
    if not generated_sql:
        print(f"  ✗ Failed to generate SQL from IQ")
        return (False, False, False, statement)
    
    print(f"  ✓ Generated SQL")
    
    # Save SQL if requested
    sql_saved = False
    if save_sql:
        sql_file = OUTPUT_SQL_DIR / f"{qId}.sql"
        try:
            with open(sql_file, 'w') as f:
                f.write(generated_sql)
            print(f"  ✓ Saved SQL: {sql_file.name}")
            sql_saved = True
        except Exception as e:
            print(f"  ✗ Failed to save SQL: {e}")
    
    # Execute SQL and save CSV if requested
    csv_saved = False
    if execute_sql:
        print(f"  Executing SQL...")
        results = exec_sql(
            sql_query=generated_sql,
            couchbase_user=couchbase_user,
            couchbase_pass=couchbase_pass,
            query_endpoint=query_endpoint
        )
        
        if results is None:
            print(f"  ✗ Failed to execute SQL")
            return (False, sql_saved, False, statement)
        
        print(f"  ✓ Executed SQL ({len(results)} rows)")
        
        # Convert to CSV
        csv_file = OUTPUT_CSV_DIR / f"{qId}.csv"
        if json_to_csv(results, csv_file):
            print(f"  ✓ Saved CSV: {csv_file.name}")
            csv_saved = True
        else:
            print(f"  ✗ Failed to save CSV")
    
    return (True, sql_saved, csv_saved, statement)


def main():
    print("="*80)
    print("IQ+BIRD Single Question Test Script")
    print("="*80)

    # Get question IDs from command line or QUESTION_IDS constant
    if len(sys.argv) > 1:
        try:
            question_ids = [int(arg) for arg in sys.argv[1:]]
        except ValueError:
            print("Error: All arguments must be valid question IDs (integers)")
            print(f"Usage: python {sys.argv[0]} <question_id1> <question_id2> ...")
            sys.exit(1)
    elif QUESTION_IDS:
        question_ids = QUESTION_IDS
    else:
        print("Error: No question IDs specified!")
        print(f"Usage: python {sys.argv[0]} <question_id1> <question_id2> ...")
        print("Or set QUESTION_IDS list in the script")
        sys.exit(1)

    print(f"Processing question IDs: {question_ids}")
    print()

    config=load_config()
    print()

    couchbase_config = config.get("couchbase", {})
    couchbase_iq_config = config.get("couchbase_iq", {})
    eval_config = config.get("evaluation", {})

    couchbase_user = couchbase_config.get("username", "Administrator")
    couchbase_pass = couchbase_config.get("password", "password")
    query_endpoint = resolve_query_endpoint_from_config(couchbase_config, couchbase_user, couchbase_pass)
    natural_cred = couchbase_iq_config.get("natural_cred", "<capella_username:password>")
    natural_orgid = couchbase_iq_config.get("natural_orgid", "<capella_orgid>")
    save_sql = eval_config.get("save_sql", True)
    execute_sql_flag = eval_config.get("execute_sql", True)
    delay = eval_config.get("delay_between_requests", 0.5)

    OUTPUT_DIR.mkdir(exist_ok=True)
    OUTPUT_SQL_DIR.mkdir(exist_ok=True)
    OUTPUT_CSV_DIR.mkdir(exist_ok=True)
    print(f"✓ Output directories created:")
    print(f"  SQL: {OUTPUT_SQL_DIR}")
    print(f"  CSV: {OUTPUT_CSV_DIR}")
    print()
    
    # Initialize failed questions log with run header
    failed_file = OUTPUT_DIR / "failed_questions.log"
    write_run_header(failed_file)
    print(f"✓ Failure log initialized: {failed_file}")
    print()

    print("Loading manual keyspace mappings...")
    manual_keyspaces_map = load_manual_keyspaces()
    if manual_keyspaces_map:
        print(f"✓ Loaded {len(manual_keyspaces_map)} manual keyspace mappings")
    else:
        print("✓ No manual keyspace mappings found (will use automatic detection)")
    print()

    print("Initializing Couchbase IQ API...")
    couchbase_iq = CouchbaseIqAPI(
        query_endpoint=query_endpoint,
        natural_cred=natural_cred,
        natural_orgid=natural_orgid,
        couchbase_user=couchbase_user,
        couchbase_pass=couchbase_pass
    )
    print("✓ Couchbase IQ API initialized")
    print()

    print("Loading questions...")
    questions = loadQuestions(question_ids=question_ids)
    
    if not questions:
        print(f"✗ No questions found with IDs: {question_ids}")
        sys.exit(1)
    
    print(f"✓ Loaded {len(questions)} question(s)")
    
    # Show which questions were found/not found
    found_ids = {q['question_id'] for q in questions}
    missing_ids = set(question_ids) - found_ids
    if missing_ids:
        print(f"  Warning: Question IDs not found: {sorted(missing_ids)}")
    print()

    # Process each question
    print("Processing questions...")
    print("-" * 80)
    
    stats = {
        'total': 0,
        'sql_generated': 0,
        'sql_saved': 0,
        'csv_saved': 0,
        'failed': 0
    }
    
    for i, item in enumerate(questions, 1):
        qId = item['question_id']
        question = item['question']
        db_id = item['db_id']
        
        stats['total'] += 1
        
        print(f"\n[{i}/{len(questions)}] {qId}")
        print(f"  Question: {question[:80]}...")
        print(f"  Database: {db_id}")
        
        try:
            success, sql_saved, csv_saved, statement = process_question(
                item=item,
                couchbase_iq=couchbase_iq,
                couchbase_user=couchbase_user,
                couchbase_pass=couchbase_pass,
                query_endpoint=query_endpoint,
                manual_keyspaces_map=manual_keyspaces_map,
                save_sql=save_sql,
                execute_sql=execute_sql_flag
            )
            
            if success:
                stats['sql_generated'] += 1
            if sql_saved:
                stats['sql_saved'] += 1
            if csv_saved:
                stats['csv_saved'] += 1
            if not success:
                stats['failed'] += 1
                log_failure_immediately(
                    failed_file=failed_file,
                    question_id=qId,
                    question=question,
                    db_id=db_id,
                    reason='Failed to generate SQL from IQ',
                    statement=statement
                )
        except Exception as e:
            import traceback
            error_details = f"{type(e).__name__}: {str(e)}"
            print(f"  ✗ Unexpected error: {error_details}")
            print(f"     Traceback: {traceback.format_exc()}")
            stats['failed'] += 1
            log_failure_immediately(
                failed_file=failed_file,
                question_id=qId,
                question=question,
                db_id=db_id,
                reason=f'Exception: {error_details}\n{traceback.format_exc()}'
            )
        
        # Small delay to avoid overwhelming the API
        if i < len(questions):  # Don't delay after last question
            time.sleep(delay)
    
    # Print summary
    print()
    print("-" * 80)
    print("Summary:")
    print(f"  Total questions: {stats['total']}")
    print(f"  SQL generated: {stats['sql_generated']}")
    print(f"  SQL saved: {stats['sql_saved']}")
    print(f"  CSV saved: {stats['csv_saved']}")
    print(f"  Failed: {stats['failed']}")
    
    if stats['failed'] > 0:
        print(f"\n⚠️  See {failed_file} for failure details")
    
    print()
    print("✓ Done!")


if __name__ == "__main__":
    main()
