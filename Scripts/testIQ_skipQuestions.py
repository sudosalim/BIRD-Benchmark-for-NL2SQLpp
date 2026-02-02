import json
from pathlib import Path
import requests
from typing import List, Dict, Optional
import time
import csv
import re
from datetime import datetime, timedelta

SCRIPT_DIR = Path(__file__).parent
CONFIG_FILE = SCRIPT_DIR.parent / "IQ_config.json"
MANUAL_KEYSPACES_FILE = SCRIPT_DIR / "manual_keyspaces.json"
OUTPUT_DIR = SCRIPT_DIR / "couchbase_iq_results"
OUTPUT_SQL_DIR = OUTPUT_DIR / "sql_outputs"
OUTPUT_CSV_DIR = OUTPUT_DIR / "csv_outputs"
JSON_OUTPUT_DIR = SCRIPT_DIR / "sqlLiteToJson" / "json_outputs"

# Manual skip list - Add question IDs here to skip them
# Example: SKIP_LIST = ["1025", "1030", "1031"]
SKIP_LIST = ["1524", "1387", "1148", "880", "896", "937", "723", "751", "766", "769", "549", "592", "598", "604", "639", "683", "685", "687", "694", "701", "710", "716", "345", "377", "402", "415", "422", "424", "440", "459", "462", "468", "472", "474", "486", "487", "518", "522", "528", "529", "530","219", "273", "12", "24", "25", "26", "28", "31", "37", "36", "39", "41", "46", "48", "72", "77", "79", "82", "83", "87"]


# Default Configuration (overridden by couchbase_config.json if exists)
DEFAULT_CONFIG = {
    "couchbase": {
        "connection_string": "couchbase://localhost",
        "username": "Administrator",
        "password": "password",
        "query_endpoint": "http://localhost:8093/query/service"
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

    def generate_sql(self, keyspaces: List[str], question: str, evidence: Optional[str] = None) -> tuple:
        """
        Generate SQL using Couchbase IQ.
        
        Args:
            keyspaces: List of keyspace/database names
            question: Natural language question
            evidence: Optional additional context/evidence
            
        Returns:
            Tuple of (generated_sql, statement) where statement is the full request sent to IQ
        """
        print(f"================================================= SS ================================================")
        with_params = {
            "keyspaces": keyspaces,
            "execute": False
        }

        # Add external_knowledge_context if evidence is provided
        # if evidence:
        #     with_params["evidence"] = evidence

        # Build the statement with the WITH clause
        with_clause_json = json.dumps(with_params)
        statement = f"USING AI WITH {with_clause_json} /*{evidence}*/ \"{question}\""
        
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

        response = None
        try:
            print("SS:  Sending request to Couchbase IQ...")
            response = requests.post(self.query_endpoint, json=payload, headers=headers)
            response.raise_for_status()
            print(f"SS: Response: {response}")
            data = response.json()
            print(f"SS: Data: {data}")
            return (data.get("generated_statement", ""), statement)
        except requests.exceptions.RequestException as e:
            print(f"  ✗ Error generating SQL: {e}")
            if response:
                try:
                    print(f"  Response: {response.json()}")
                except:
                    print(f"  Response text: {response.text}")
            return ("", statement)

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
        List of result rows as dictionaries
    """
    payload = {
        "statement": sql_query,
    }
    
    headers = {
        "Content-Type": "application/json",
        "Authorization": requests.auth._basic_auth_str(couchbase_user, couchbase_pass)
    }
    
    response = None
    try:
        response = requests.post(query_endpoint, json=payload, headers=headers)
        response.raise_for_status()
        data = response.json()
        return data.get("results", [])
    except requests.exceptions.RequestException as e:
        print(f"  ✗ Error executing SQL: {e}")
        if response:
            try:
                error_data = response.json()
                print(f"  Error details: {error_data}")
            except:
                print(f"  Response text: {response.text}")
        return []


def json_to_csv(json_results: List[Dict], output_file: Path) -> bool:
    """
    Convert JSON results to CSV format.
    
    Args:
        json_results: List of result dictionaries
        output_file: Path to output CSV file
        
    Returns:
        True if successful, False otherwise
    """
    if not json_results:
        # Empty result - create CSV with no data
        with open(output_file, 'w', newline='') as f:
            f.write('')
        return True
    
    try:
        # Get all unique keys from all results
        all_keys = set()
        for result in json_results:
            # Handle nested dictionaries (Couchbase results might be nested)
            if isinstance(result, dict):
                for key, value in result.items():
                    if isinstance(value, dict):
                        # Flatten nested dict
                        for nested_key in value.keys():
                            all_keys.add(f"{key}.{nested_key}")
                    else:
                        all_keys.add(key)
        
        if not all_keys:
            # No keys found - empty result
            with open(output_file, 'w', newline='') as f:
                f.write('')
            return True
        
        fieldnames = sorted(all_keys)
        
        with open(output_file, 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            
            for result in json_results:
                row = {}
                for key, value in result.items():
                    if isinstance(value, dict):
                        # Flatten nested dict
                        for nested_key, nested_value in value.items():
                            row[f"{key}.{nested_key}"] = nested_value
                    else:
                        row[key] = value
                writer.writerow(row)
        
        return True
    except Exception as e:
        print(f"  ✗ Error converting to CSV: {e}")
        return False        

def load_config():
    """Load configuration from json file or user defaults"""
    if CONFIG_FILE.exists():
        print(f"Loading configuration from {CONFIG_FILE.name}")
        with open(CONFIG_FILE,"r") as f:
            config=json.load(f)
        return config
    else:
        print("Configuration file not found. Using default settings.")
        return DEFAULT_CONFIG

def get_ist_timestamp():
    """Get current time in IST (UTC+5:30)"""
    utc_now = datetime.utcnow()
    ist_offset = timedelta(hours=5, minutes=30)
    ist_now = utc_now + ist_offset
    return ist_now.strftime("%Y-%m-%d %H:%M:%S IST")

def log_failure_immediately(failed_file: Path, question_id: str, question: str, db_id: str, reason: str, statement: str = None):
    """Append a failed question to the log file immediately."""
    try:
        failure_entry = {
            'timestamp': get_ist_timestamp(),
            'question_id': question_id,
            'question': question,
            'db_id': db_id,
            'reason': reason
        }
        
        # Add statement if provided
        if statement:
            failure_entry['statement'] = statement
        
        # Append to file (create if doesn't exist)
        with open(failed_file, 'a') as f:
            f.write(json.dumps(failure_entry) + '\n')
    except Exception as e:
        print(f"  Warning: Could not log failure to file: {e}")

def write_run_header(failed_file: Path):
    """Write a run header with IST timestamp to the failed questions log."""
    try:
        header = {
            'type': 'RUN_START',
            'timestamp': get_ist_timestamp(),
            'message': 'New run started'
        }
        
        with open(failed_file, 'a') as f:
            f.write('\n' + '='*80 + '\n')
            f.write(json.dumps(header) + '\n')
            f.write('='*80 + '\n')
    except Exception as e:
        print(f"  Warning: Could not write run header: {e}")

def loadQuestions():
    with open(
        "/Users/aayush.fabwani/Documents/Projects/Benchmark/Work/mini_dev/mini_dev_data/mini_dev_sqlite.json",
        "r"
    ) as f:
        dataset = json.load(f)
    # Load Questions
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

def get_keyspaces_for_db(db_id: str, qId: str, question: str, evidence: str) -> List[str]:
    """
    Get keyspace names for a given database.
    Returns the bucket.scope format for Couchbase.
    
    Args:
        db_id: Database identifier
        qId: Question ID
        question: Natural language question
        evidence: Additional context/evidence
        
    Returns:
        List of keyspace names (max 4)
    """
    # For BIRD mini-dev, we use bucket name as db_id with bird scope
    # Format: bucket.scope.collection

    # Get collections with their fields
    collections_dict = get_all_collections_in_scope(
        bucket_name=db_id,
        scope_name="bird",
    )
    
    # Select top 4 relevant collections based on question and schema
    selected_collections = reduce_keyspace_via_collection(
        [question, evidence], 
        collections_dict,
        max_keyspaces=4
    )
    
    # Format as keyspaces
    keyspaces = []
    for coll in selected_collections:
        keyspaces.append(f"`{db_id}`.`bird`.`{coll}`")
    
    return keyspaces

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
        print(f"  ✗ Failed to generate SQL")
        return (False, False, False, statement)

    print(f"  ✓ Generated SQL ({len(generated_sql)} chars)")

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
    """Main function to test IQ endpoint"""
    print("="*80)
    print("IQ+BIRD Mini Dev Test Script")
    print("="*80)

    config=load_config()
    print()

    couchbase_config = config.get("couchbase", {})
    couchbase_iq_config = config.get("couchbase_iq", {})
    eval_config = config.get("evaluation", {})

    query_endpoint = couchbase_config.get("query_endpoint", "http://localhost:8093/query/service")
    couchbase_user = couchbase_config.get("username", "Administrator")
    couchbase_pass = couchbase_config.get("password", "password")
    natural_cred = couchbase_iq_config.get("natural_cred", "<capella_username:password>")
    natural_orgid = couchbase_iq_config.get("natural_orgid", "<capella_orgid>")
    max_questions = eval_config.get("max_questions", None)
    save_sql = eval_config.get("save_sql", True)
    execute_sql_flag = eval_config.get("execute_sql", True)
    delay = eval_config.get("delay_between_requests", 0.5)
    rerun_existing = eval_config.get("rerun_existing", False)

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
    questions = loadQuestions()
    total_questions = len(questions)
    
    if max_questions:
        questions = questions[:max_questions]
        print(f"✓ Loaded {total_questions} questions (processing {len(questions)})")
    else:
        print(f"✓ Loaded {total_questions} questions")
    print()

    # Process each question
    print("Processing questions...")
    print("-" * 80)
    
    stats = {
        'total': 0,
        'sql_generated': 0,
        'sql_saved': 0,
        'csv_saved': 0,
        'failed': 0,
        'skipped': 0,
        'manually_skipped': 0
    }
    
    for i,item in enumerate(questions,1):
        qId=item['question_id']
        question=item['question']
        evidence=item.get('evidence',None)
        db_id=item['db_id']

        print(f"\n[{i}/{len(questions)}] {qId}")
        print(f"  Question: {question[:80]}...")
        print(f"  Database: {db_id}")
        stats['total'] += 1

        # Check if question is in manual skip list
        if str(qId) in SKIP_LIST:
            print(f"  ⊗ Skipping (in manual skip list)")
            stats['manually_skipped'] += 1
            continue

        # Check if outputs already exist
        sql_file = OUTPUT_SQL_DIR / f"{qId}.sql"
        csv_file = OUTPUT_CSV_DIR / f"{qId}.csv"
        
        if not rerun_existing:
            sql_exists = sql_file.exists() if save_sql else True
            csv_exists = csv_file.exists() if execute_sql_flag else True
            
            if sql_exists and csv_exists:
                print(f"  ⊙ Skipping (already processed)")
                stats['skipped'] += 1
                continue

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
        time.sleep(delay)
    
    # Print summary
    print()
    print("-" * 80)
    print("Summary:")
    print(f"  Total questions: {stats['total']}")
    print(f"  Manually skipped (skip list): {stats['manually_skipped']}")
    print(f"  Skipped (already processed): {stats['skipped']}")
    print(f"  SQL generated: {stats['sql_generated']}")
    print(f"  SQL saved: {stats['sql_saved']}")
    print(f"  CSV saved: {stats['csv_saved']}")
    print(f"  Failed: {stats['failed']}")
    
    # Reference failed questions log if there were failures
    if stats['failed'] > 0:
        print(f"\n✓ Failed questions logged to: {failed_file}")
        print(f"  View the log file for details on {stats['failed']} failed question(s)")
    
    print()
    print("=" * 80)
    print("Next Steps:")
    print("=" * 80)
    print()
    print("1. Review generated files:")
    print(f"   SQL: {OUTPUT_SQL_DIR}")
    print(f"   CSV: {OUTPUT_CSV_DIR}")
    print()
    print("2. Run evaluation:")
    print(f"   # Using SQL mode:")
    print(f"   python3 evaluate.py --result_dir {OUTPUT_SQL_DIR.name} --mode sql")
    print()
    print(f"   # Using CSV mode:")
    print(f"   python3 evaluate.py --result_dir {OUTPUT_CSV_DIR.name} --mode exec_result")
    print()
        
if __name__ == "__main__":
    main()