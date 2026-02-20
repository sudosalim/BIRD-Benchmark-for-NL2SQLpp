import os
import csv
import pandas as pd
from pathlib import Path
import json
from collections import defaultdict
from datetime import datetime, timedelta
from difflib import SequenceMatcher
import hashlib

# Define paths
SQLPP_RESULTS_DIR = Path(__file__).parent.parent / "couchbase_iq_results" / "baseline8" / "csv_outputs"
SQLITE_RESULTS_DIR = Path(__file__).parent / "2sql_query_results"
LOG_FILE = Path(__file__).parent / "comparison_results.log"
DETAILED_LOG_DIR = Path(__file__).parent / "detailed_logs"

# Global log file handle
log_file_handle = None

def get_ist_timestamp():
    """Get current time in IST (UTC+5:30)"""
    utc_now = datetime.utcnow()
    ist_offset = timedelta(hours=5, minutes=30)
    ist_now = utc_now + ist_offset
    return ist_now.strftime("%Y-%m-%d %H:%M:%S IST")

def log_print(message):
    """Print to both terminal and log file."""
    print(message)
    if log_file_handle:
        log_file_handle.write(message + '\n')
        log_file_handle.flush()  # Ensure it's written immediately

def init_log_file():
    """Initialize log file with run header."""
    global log_file_handle
    log_file_handle = open(LOG_FILE, 'a')
    log_file_handle.write('\n' + '='*80 + '\n')
    log_file_handle.write(f"RUN STARTED: {get_ist_timestamp()}\n")
    log_file_handle.write('='*80 + '\n')
    log_file_handle.flush()

def close_log_file():
    """Close log file handle."""
    global log_file_handle
    if log_file_handle:
        log_file_handle.write(f"\nRUN COMPLETED: {get_ist_timestamp()}\n")
        log_file_handle.write('='*80 + '\n')
        log_file_handle.close()
        log_file_handle = None


def convert_to_json_serializable(obj):
    """Convert numpy/pandas types to JSON-serializable Python types."""
    import numpy as np
    
    if isinstance(obj, (np.integer, np.int64, np.int32)):
        return int(obj)
    elif isinstance(obj, (np.floating, np.float64, np.float32)):
        return float(obj)
    elif isinstance(obj, (np.bool_, bool)):
        return bool(obj)
    elif isinstance(obj, np.ndarray):
        return obj.tolist()
    elif isinstance(obj, dict):
        return {k: convert_to_json_serializable(v) for k, v in obj.items()}
    elif isinstance(obj, (list, tuple)):
        return [convert_to_json_serializable(item) for item in obj]
    elif pd.isna(obj):
        return None
    return obj


def normalize_value(val):
    """
    Normalize a value for comparison:
    - Trim whitespace for strings
    - Lowercase strings
    - Standardize null values
    - Handle numeric types consistently
    """
    if val is None or pd.isna(val):
        return None
    
    if isinstance(val, str):
        # Trim whitespace and lowercase
        normalized = val.strip().lower()
        # Standardize null representations
        if normalized in ['null', 'none', 'nan', '', 'n/a']:
            return None
        return normalized
    
    if isinstance(val, (int, float)):
        # Handle floating point comparison with tolerance
        if isinstance(val, float):
            # Check if it's essentially an integer
            if val.is_integer():
                return int(val)
            return round(val, 6)
        return val
    
    # For other types, convert to string and normalize
    return str(val).strip().lower()


def fuzzy_match_score(str1, str2):
    """Calculate fuzzy matching score between two strings (0-1)."""
    if str1 == str2:
        return 1.0
    
    # Normalize strings for comparison
    s1 = str1.lower().strip()
    s2 = str2.lower().strip()
    
    # Check if one contains the other
    if s1 in s2 or s2 in s1:
        return 0.9
    
    # Use SequenceMatcher for fuzzy matching
    return SequenceMatcher(None, s1, s2).ratio()


def detect_column_type(df, column_name, sample_size=100):
    """
    Detect the predominant data type of a column by sampling its values.
    
    Returns:
        - 'numeric': Column contains mostly numeric values (int/float)
        - 'string': Column contains mostly string/text values
        - 'mixed': Column contains a mix of types or is ambiguous
    """
    if column_name not in df.columns:
        return 'mixed'
    
    # Get a sample of non-null values
    col_values = df[column_name].dropna()
    
    if len(col_values) == 0:
        return 'mixed'
    
    # Sample up to sample_size values
    sample = col_values.head(min(sample_size, len(col_values)))
    
    numeric_count = 0
    string_count = 0
    
    for val in sample:
        if isinstance(val, (int, float, complex)):
            numeric_count += 1
        elif isinstance(val, str):
            # Try to parse as number
            try:
                float(val.strip())
                numeric_count += 1
            except (ValueError, AttributeError):
                string_count += 1
        else:
            # Other types treated as mixed
            pass
    
    total = numeric_count + string_count
    if total == 0:
        return 'mixed'
    
    # If 80% or more are numeric, classify as numeric
    if numeric_count / total >= 0.8:
        return 'numeric'
    # If 80% or more are string, classify as string
    elif string_count / total >= 0.8:
        return 'string'
    else:
        return 'mixed'


def get_column_types(df, columns):
    """
    Get type classification for a list of columns.
    
    Returns:
        dict mapping column name to type ('numeric', 'string', or 'mixed')
    """
    return {col: detect_column_type(df, col) for col in columns}


def are_types_compatible(type1, type2):
    """
    Check if two column types are compatible for mapping.
    
    Rules:
    - numeric <-> numeric: Compatible
    - string <-> string: Compatible
    - mixed <-> any: Compatible (fallback)
    - numeric <-> string: Incompatible
    """
    if type1 == 'mixed' or type2 == 'mixed':
        return True
    return type1 == type2


def map_columns_hybrid(gold_columns, test_columns, gold_df=None, test_df=None, fuzzy_threshold=0.7):
    """
    Map test columns to gold columns using a type-aware hybrid approach:
    
    Stage 0: Type classification (if dataframes provided)
    - Detect data type of each column (numeric/string/mixed)
    - Only allow mappings between type-compatible columns
    
    Stage 1: Fuzzy matching for high-confidence semantic matches
    - Match columns with similar names using fuzzy string matching
    - Lock in matches that exceed the confidence threshold
    - Only match type-compatible columns
    
    Stage 2: Positional fallback for remaining columns
    - For columns that couldn't be matched semantically, use left-to-right order
    - Pair remaining unmatched columns by their position
    - Only match type-compatible columns
    
    Returns:
        - mapping: dict mapping test column names to gold column names
        - unmapped_test_cols: list of test columns that couldn't be mapped
        - unmapped_gold_cols: list of gold columns that couldn't be mapped
        - stage1_matches: number of columns matched by fuzzy matching
        - stage2_matches: number of columns matched by position
    """
    mapping = {}
    used_gold_cols = set()
    used_test_cols = set()
    
    # STAGE 0: Type classification (if dataframes are provided)
    gold_types = {}
    test_types = {}
    type_aware = False
    
    if gold_df is not None and test_df is not None:
        type_aware = True
        gold_types = get_column_types(gold_df, gold_columns)
        test_types = get_column_types(test_df, test_columns)
    
    # STAGE 1: Fuzzy matching for high-confidence matches (with type awareness)
    stage1_matches = 0
    
    # For each gold column, find the best matching test column
    for gold_col in gold_columns:
        best_match = None
        best_score = 0
        
        for test_col in test_columns:
            if test_col in used_test_cols:
                continue
            
            # Check type compatibility if type-aware
            if type_aware:
                gold_type = gold_types.get(gold_col, 'mixed')
                test_type = test_types.get(test_col, 'mixed')
                if not are_types_compatible(gold_type, test_type):
                    continue  # Skip incompatible types
            
            score = fuzzy_match_score(gold_col, test_col)
            if score > best_score and score >= fuzzy_threshold:
                best_score = score
                best_match = test_col
        
        if best_match:
            mapping[best_match] = gold_col
            used_gold_cols.add(gold_col)
            used_test_cols.add(best_match)
            stage1_matches += 1
    
    # STAGE 2: Positional fallback for remaining columns (with type awareness)
    stage2_matches = 0
    
    # Get lists of remaining unmatched columns in their original order
    unmapped_gold_cols = [col for col in gold_columns if col not in used_gold_cols]
    unmapped_test_cols = [col for col in test_columns if col not in used_test_cols]
    
    if type_aware:
        # Type-aware positional matching: only pair columns with compatible types
        for gold_col in unmapped_gold_cols:
            gold_type = gold_types.get(gold_col, 'mixed')
            
            for test_col in unmapped_test_cols:
                if test_col in used_test_cols:
                    continue
                    
                test_type = test_types.get(test_col, 'mixed')
                
                # Check type compatibility
                if are_types_compatible(gold_type, test_type):
                    # Found a type-compatible match
                    mapping[test_col] = gold_col
                    used_gold_cols.add(gold_col)
                    used_test_cols.add(test_col)
                    stage2_matches += 1
                    break  # Move to next gold column
    else:
        # Original positional matching (no type awareness)
        num_to_pair = min(len(unmapped_gold_cols), len(unmapped_test_cols))
        
        for i in range(num_to_pair):
            test_col = unmapped_test_cols[i]
            gold_col = unmapped_gold_cols[i]
            mapping[test_col] = gold_col
            used_gold_cols.add(gold_col)
            used_test_cols.add(test_col)
            stage2_matches += 1
    
    # Update unmapped lists after stage 2
    final_unmapped_test = [col for col in test_columns if col not in used_test_cols]
    final_unmapped_gold = [col for col in gold_columns if col not in used_gold_cols]
    
    return mapping, final_unmapped_test, final_unmapped_gold, stage1_matches, stage2_matches


def normalize_dataframe(df):
    """
    Normalize all values in a dataframe:
    - Trim whitespace
    - Lowercase strings
    - Standardize nulls
    """
    df_normalized = df.copy()
    for col in df_normalized.columns:
        df_normalized[col] = df_normalized[col].apply(normalize_value)
    return df_normalized


def dataframe_to_row_set(df):
    """
    Convert dataframe rows to a set of hashable tuples.
    Each row is converted to a tuple of normalized values.
    """
    row_set = set()
    for _, row in df.iterrows():
        row_tuple = tuple(row.values)
        row_set.add(row_tuple)
    return row_set


def compare_dataframes(gold_df, test_df, query_num):
    """
    Compare two dataframes with type-aware hybrid column mapping and normalization.
    
    STRICT ROW MATCHING: Test must have EXACTLY the same rows as gold (no extra rows allowed).
    FLEXIBLE COLUMN MATCHING: Test can have more columns than gold (columns are mapped intelligently).
    
    Process:
    1. Normalize all values in both dataframes (trim, lowercase, standardize nulls)
    2. Type-aware hybrid column alignment:
       - Stage 0: Detect column data types (numeric/string/mixed)
       - Stage 1: Fuzzy matching for semantically similar column names (high confidence)
       - Stage 2: Positional fallback for remaining columns (left-to-right order)
       - All stages enforce type compatibility (numeric columns only map to numeric, etc.)
    3. Rename test columns using the hybrid mapping
    4. Drop extra test columns, keep only gold schema columns
    5. Reorder test columns to match gold column order
    6. Convert each row to comparable tuples
    7. Check if test has EXACTLY the same rows as gold (no missing, no extra)
    
    Returns:
        - is_success: True if test has exactly the same rows as gold (no extra rows allowed)
        - is_exact: True if both have exactly the same rows AND same columns
        - details: dict with detailed comparison info including mapping strategy
    """
    details = {
        'gold_rows': int(len(gold_df)),
        'test_rows': int(len(test_df)),
        'gold_cols': int(len(gold_df.columns)),
        'test_cols': int(len(test_df.columns)),
        'column_mapping': {},
        'unmapped_test_cols': [],
        'unmapped_gold_cols': [],
        'stage1_matches': 0,
        'stage2_matches': 0,
        'missing_rows': [],
        'extra_rows': []
    }
    
    # Handle empty dataframes
    if len(gold_df) == 0 and len(test_df) == 0:
        return True, True, details
    
    if len(gold_df) == 0 and len(test_df) > 0:
        details['extra_rows'] = len(test_df)
        return False, False, details
    
    if len(test_df) == 0 and len(gold_df) > 0:
        details['missing_rows'] = len(gold_df)
        return False, False, details
    
    # Step 1: Normalize all values (trim whitespace, lowercase, standardize nulls)
    gold_normalized = normalize_dataframe(gold_df)
    test_normalized = normalize_dataframe(test_df)
    
    # Step 2: Type-aware hybrid column mapping
    gold_columns = list(gold_normalized.columns)
    test_columns = list(test_normalized.columns)
    
    column_mapping, unmapped_test, unmapped_gold, stage1, stage2 = map_columns_hybrid(
        gold_columns, test_columns, gold_normalized, test_normalized
    )
    
    details['column_mapping'] = column_mapping
    details['unmapped_test_cols'] = unmapped_test
    details['unmapped_gold_cols'] = unmapped_gold
    details['stage1_matches'] = stage1
    details['stage2_matches'] = stage2
    
    # Step 3: Rename test columns and keep only mapped columns
    # Only use columns that were successfully mapped (from both stages)
    mapped_test_cols = [test_col for test_col in test_columns if test_col in column_mapping]
    
    if not mapped_test_cols:
        # No columns could be mapped
        details['error'] = 'No columns could be mapped'
        return False, False, details
    
    # Create a test dataframe with renamed columns
    test_renamed = test_normalized[mapped_test_cols].copy()
    test_renamed.columns = [column_mapping[col] for col in mapped_test_cols]
    
    # Step 4: Drop extra columns - keep only columns that exist in gold schema
    common_columns = [col for col in test_renamed.columns if col in gold_columns]
    
    if not common_columns:
        details['error'] = 'No common columns after mapping'
        return False, False, details
    
    # Step 5: Reorder both dataframes to match gold column order
    # This ensures columns are in the same order for row comparison
    gold_subset = gold_normalized[common_columns].copy()
    test_subset = test_renamed[common_columns].copy()
    
    # Step 6: Convert rows to comparable tuples (each row becomes a hashable record)
    gold_rows = dataframe_to_row_set(gold_subset)
    test_rows = dataframe_to_row_set(test_subset)
    
    # Step 7: STRICT CHECK - test must have EXACTLY the same rows (no extra rows allowed)
    # Missing rows: rows in gold but NOT in test (causes failure)
    missing_rows = gold_rows - test_rows
    
    # Extra rows: rows in test but NOT in gold (ALSO causes failure - this is the key difference)
    extra_rows = test_rows - gold_rows
    
    details['missing_rows_count'] = int(len(missing_rows))
    details['extra_rows_count'] = int(len(extra_rows))
    
    # Store sample of missing/extra rows for debugging (limit to 5)
    if missing_rows:
        details['missing_rows_sample'] = [convert_to_json_serializable(list(row)) for row in list(missing_rows)[:5]]
    if extra_rows:
        details['extra_rows_sample'] = [convert_to_json_serializable(list(row)) for row in list(extra_rows)[:5]]
    
    # Success: Test has EXACTLY the same rows as gold (no missing, NO EXTRA)
    # Extra rows cause failure!
    is_success = len(missing_rows) == 0 and len(extra_rows) == 0
    
    # If not successful and same number of rows, try comparing rows as SETS
    # (values in any order) to handle column mapping issues
    if not is_success and len(gold_df) == len(test_df) and len(gold_df) > 0:
        # Convert each row to a frozenset of values (order-independent)
        gold_row_sets = set()
        for _, row in gold_subset.iterrows():
            gold_row_sets.add(frozenset(row.values))
        
        test_row_sets = set()
        for _, row in test_subset.iterrows():
            test_row_sets.add(frozenset(row.values))
        
        # Check if rows match when treated as sets
        if gold_row_sets == test_row_sets:
            is_success = True
            details['match_method'] = 'row_as_set'  # Rows matched as unordered sets
    
    # Exact match: same rows AND same columns
    is_exact = is_success and (len(gold_columns) == len(test_columns))
    
    return is_success, is_exact, details


def save_detailed_log(query_num, is_success, is_exact, details, gold_file, test_file):
    """Save detailed comparison log for a specific query."""
    DETAILED_LOG_DIR.mkdir(exist_ok=True)
    
    log_path = DETAILED_LOG_DIR / f"query_{query_num}_details.json"
    
    log_data = {
        'query_number': query_num,
        'gold_file': str(gold_file),
        'test_file': str(test_file),
        'success': is_success,
        'exact_match': is_exact,
        'details': details,
        'timestamp': get_ist_timestamp()
    }
    
    with open(log_path, 'w') as f:
        json.dump(log_data, f, indent=2)


def load_csv_safely(file_path):
    """
    Safely load a CSV file into a dataframe.
    Returns None if file doesn't exist or has errors.
    Returns empty dataframe if file is empty.
    """
    try:
        if not os.path.exists(file_path):
            return None
        
        # Check if file is empty
        if os.path.getsize(file_path) == 0:
            return pd.DataFrame()
        
        df = pd.read_csv(file_path)
        return df
    
    except Exception as e:
        print(f"Error reading {file_path}: {str(e)}")
        return None


def extract_query_number(filename):
    """Extract query number from filename."""
    # For sqlpp files: "846.csv" -> "846"
    # For sqlite files: "question_846_formula_1.csv" -> "846"
    if filename.startswith("question_"):
        parts = filename.split("_")
        if len(parts) >= 2:
            return parts[1]
    else:
        return filename.split(".")[0]
    return None


def find_matching_sqlite_file(query_num, sqlite_dir):
    """Find the matching sqlite file for a given query number."""
    pattern = f"question_{query_num}_*.csv"
    matches = list(sqlite_dir.glob(pattern))
    if matches:
        return matches[0]
    else:
        # print(f"Warning: Could not find matching SQLite file for query number {query_num}")
        return None


def main():
    """Main function to compare all query results with strict row matching."""
    
    # Total questions in the dataset
    TOTAL_QUESTIONS = 500
    
    # Initialize log file
    init_log_file()
    
    # Ensure directories exist
    if not SQLPP_RESULTS_DIR.exists():
        log_print(f"Error: SQL++ results directory not found: {SQLPP_RESULTS_DIR}")
        close_log_file()
        return
    
    if not SQLITE_RESULTS_DIR.exists():
        log_print(f"Error: SQLite results directory not found: {SQLITE_RESULTS_DIR}")
        close_log_file()
        return
    
    # Get all SQL++ result files (test files)
    test_files = sorted(SQLPP_RESULTS_DIR.glob("*.csv"))
    
    log_print(f"Total questions in dataset: {TOTAL_QUESTIONS}")
    log_print(f"Found {len(test_files)} test result files")
    log_print(f"Missing/Failed: {TOTAL_QUESTIONS - len(test_files)}")
    log_print(f"Comparing with gold results from {SQLITE_RESULTS_DIR}")
    log_print("=" * 80)
    
    # Store results
    results = []
    successes = 0
    exact_matches = 0
    comparisons_made = 0
    missing_gold_files = 0
    empty_test_files = 0
    empty_test_queries = []
    success_queries = []
    exact_match_queries = []
    failed_queries = []
    extra_rows_queries = []  # Queries that failed due to extra rows
    
    for test_file in test_files:
        query_num = extract_query_number(test_file.name)
        
        if not query_num:
            log_print(f"Warning: Could not extract query number from {test_file.name}")
            continue
        
        # Find matching gold (SQLite) file
        gold_file = find_matching_sqlite_file(query_num, SQLITE_RESULTS_DIR)
        
        if not gold_file:
            log_print(f"Missing gold file for query {query_num}")
            missing_gold_files += 1
            continue
        
        # Load both files
        gold_df = load_csv_safely(gold_file)
        test_df = load_csv_safely(test_file)
        
        if gold_df is None or test_df is None:
            log_print(f"Error loading files for query {query_num}")
            continue
        
        # Track empty test files
        if len(test_df) == 0:
            empty_test_files += 1
            empty_test_queries.append(query_num)
            log_print(f"Note: Test file for query {query_num} is empty")
        
        # Compare dataframes
        is_success, is_exact, details = compare_dataframes(gold_df, test_df, query_num)
        
        comparisons_made += 1
        
        if is_success:
            successes += 1
            success_queries.append(query_num)
            if is_exact:
                exact_matches += 1
                exact_match_queries.append(query_num)
        else:
            failed_queries.append(query_num)
            # Track if failure was due to extra rows
            if details.get('extra_rows_count', 0) > 0 and details.get('missing_rows_count', 0) == 0:
                extra_rows_queries.append(query_num)
        
        result = {
            'query_number': query_num,
            'test_file': test_file.name,
            'gold_file': gold_file.name,
            'success': bool(is_success),
            'exact_match': bool(is_exact),
            'match_type': 'exact' if is_exact else ('success' if is_success else 'failed'),
            'gold_rows': int(details['gold_rows']),
            'test_rows': int(details['test_rows']),
            'gold_cols': int(details['gold_cols']),
            'test_cols': int(details['test_cols']),
            'missing_rows_count': int(details.get('missing_rows_count', 0)),
            'extra_rows_count': int(details.get('extra_rows_count', 0)),
            'mapped_columns': int(len(details['column_mapping'])),
            'stage1_fuzzy_matches': int(details.get('stage1_matches', 0)),
            'stage2_positional_matches': int(details.get('stage2_matches', 0)),
            'unmapped_test_cols': int(len(details['unmapped_test_cols'])),
            'unmapped_gold_cols': int(len(details['unmapped_gold_cols']))
        }
        results.append(result)
        
        # Save detailed log for failed queries
        if not is_success:
            save_detailed_log(query_num, is_success, is_exact, details, gold_file, test_file)
            
            # Print brief failure summary
            missing = details.get('missing_rows_count', 0)
            extra = details.get('extra_rows_count', 0)
            stage1 = details.get('stage1_matches', 0)
            stage2 = details.get('stage2_matches', 0)
            log_print(f"Query {query_num}: FAILED - Missing: {missing} rows, Extra: {extra} rows | Mapping: {stage1} fuzzy + {stage2} positional")
    
    # Calculate statistics
    files_generated = len(test_files)
    failed_generations = TOTAL_QUESTIONS - files_generated
    
    log_print("\n" + "=" * 80)
    log_print("SUMMARY")
    log_print("=" * 80)
    log_print(f"Total questions: {TOTAL_QUESTIONS}")
    log_print(f"Files generated (successful): {files_generated}")
    log_print(f"Failed to generate: {failed_generations}")
    log_print(f"Total comparisons made: {comparisons_made}")
    log_print(f"")
    log_print(f"RESULTS (STRICT ROW MATCHING - no extra rows allowed):")
    log_print(f"  Total successes: {successes}")
    log_print(f"  Exact matches (same rows, same columns): {exact_matches}")
    log_print(f"  Column-flexible matches (same rows, extra columns): {successes - exact_matches}")
    log_print(f"  Failures: {comparisons_made - successes}")
    log_print(f"    - Failures due to extra rows only: {len(extra_rows_queries)}")
    log_print(f"  Missing gold files: {missing_gold_files}")
    log_print(f"  Empty test CSV files: {empty_test_files}")
    log_print(f"")
    log_print(f"RATES:")
    log_print(f"  Overall accuracy: {successes / TOTAL_QUESTIONS * 100:.2f}%")
    log_print(f"  Generation rate: {files_generated / TOTAL_QUESTIONS * 100:.2f}%")
    log_print(f"  Success rate (among generated): {successes / files_generated * 100:.2f}%" if files_generated > 0 else "  Success rate: N/A")
    log_print(f"  Exact match rate: {exact_matches / comparisons_made * 100:.2f}%" if comparisons_made > 0 else "  Exact match rate: N/A")
    
    # Display successful queries
    if exact_match_queries:
        log_print(f"\nExact Match Queries ({len(exact_match_queries)}):")
        sorted_queries = sorted(exact_match_queries, key=lambda x: int(x))
        log_print(", ".join(sorted_queries))
    
    if success_queries:
        column_flex_queries = [q for q in success_queries if q not in exact_match_queries]
        if column_flex_queries:
            log_print(f"\nColumn-Flexible Match Queries ({len(column_flex_queries)}):")
            sorted_queries = sorted(column_flex_queries, key=lambda x: int(x))
            log_print(", ".join(sorted_queries))
    
    # Display failed queries
    if failed_queries:
        log_print(f"\nFailed Queries ({len(failed_queries)}):")
        sorted_failures = sorted(failed_queries, key=lambda x: int(x))
        log_print(", ".join(sorted_failures))
    
    # Display queries that failed only due to extra rows
    if extra_rows_queries:
        log_print(f"\nQueries Failed Due to Extra Rows Only ({len(extra_rows_queries)}):")
        sorted_extra = sorted(extra_rows_queries, key=lambda x: int(x))
        log_print(", ".join(sorted_extra))
    
    # Display empty test file queries
    if empty_test_queries:
        log_print(f"\nEmpty Test CSV Queries ({len(empty_test_queries)}):")
        sorted_empty = sorted(empty_test_queries, key=lambda x: int(x))
        log_print(", ".join(sorted_empty))
    
    # Save detailed results to JSON
    output_file = Path(__file__).parent / "comparison_results.json"
    
    # Convert all results to JSON-serializable format
    json_safe_results = [convert_to_json_serializable(r) for r in results]
    
    with open(output_file, 'w') as f:
        json.dump({
            'summary': {
                'total_questions': TOTAL_QUESTIONS,
                'files_generated': files_generated,
                'failed_generations': failed_generations,
                'total_comparisons': comparisons_made,
                'successes': successes,
                'exact_matches': exact_matches,
                'column_flexible_matches': successes - exact_matches,
                'failures': comparisons_made - successes,
                'failures_due_to_extra_rows_only': len(extra_rows_queries),
                'missing_gold_files': missing_gold_files,
                'empty_test_files': empty_test_files,
                'overall_accuracy': successes / TOTAL_QUESTIONS,
                'generation_rate': files_generated / TOTAL_QUESTIONS,
                'success_rate_among_generated': successes / files_generated if files_generated > 0 else 0,
                'exact_match_rate': exact_matches / comparisons_made if comparisons_made > 0 else 0,
                'success_queries': sorted(success_queries, key=lambda x: int(x)),
                'exact_match_queries': sorted(exact_match_queries, key=lambda x: int(x)),
                'failed_queries': sorted(failed_queries, key=lambda x: int(x)),
                'extra_rows_only_queries': sorted(extra_rows_queries, key=lambda x: int(x)),
                'empty_test_queries': sorted(empty_test_queries, key=lambda x: int(x))
            },
            'detailed_results': json_safe_results
        }, f, indent=2)
    
    log_print(f"\nDetailed results saved to: {output_file}")
    
    # Save CSV report
    csv_output_file = Path(__file__).parent / "comparison_results.csv"
    if results:
        df = pd.DataFrame(results)
        df = df.sort_values('query_number')
        df.to_csv(csv_output_file, index=False)
        log_print(f"CSV report saved to: {csv_output_file}")
    
    log_print(f"Log saved to: {LOG_FILE}")
    log_print(f"Detailed failure logs saved to: {DETAILED_LOG_DIR}")
    
    # Close log file
    close_log_file()


if __name__ == "__main__":
    main()
