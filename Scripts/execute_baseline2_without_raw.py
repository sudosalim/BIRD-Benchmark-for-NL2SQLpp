#!/usr/bin/env python3
"""
Script to process SQL queries from baseline2/sql_outputs:
1. Remove all mentions of the word "RAW"
2. Execute the modified queries
3. Store results as CSV files in baseline2/csv_outputs2
"""

import json
from pathlib import Path
import requests
from typing import List, Dict, Optional
import time
import csv
import re
from datetime import datetime
from couchbase_endpoint_utils import resolve_query_endpoint_from_config

SCRIPT_DIR = Path(__file__).parent
CONFIG_FILE = SCRIPT_DIR.parent / "IQ_config.json"
BASELINE2_DIR = SCRIPT_DIR / "couchbase_iq_results" / "baseline2"
INPUT_SQL_DIR = BASELINE2_DIR / "sql_outputs"
OUTPUT_CSV_DIR = BASELINE2_DIR / "csv_outputs2"
LOG_FILE = SCRIPT_DIR / "execute_baseline2_without_raw.log"

# Global log file handle
log_file_handle = None

def get_timestamp():
    """Get current timestamp"""
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def log_print(message):
    """Print to both terminal and log file."""
    print(message)
    if log_file_handle:
        log_file_handle.write(message + '\n')
        log_file_handle.flush()

def init_log_file():
    """Initialize log file with run header."""
    global log_file_handle
    log_file_handle = open(LOG_FILE, 'a')
    log_file_handle.write('\n' + '='*80 + '\n')
    log_file_handle.write(f"RUN STARTED: {get_timestamp()}\n")
    log_file_handle.write('='*80 + '\n')
    log_file_handle.flush()

def close_log_file():
    """Close log file handle."""
    global log_file_handle
    if log_file_handle:
        log_file_handle.write(f"\nRUN COMPLETED: {get_timestamp()}\n")
        log_file_handle.write('='*80 + '\n')
        log_file_handle.close()
        log_file_handle = None

def load_config():
    """Load configuration from IQ_config.json"""
    try:
        with open(CONFIG_FILE, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        log_print(f"Error: Config file not found at {CONFIG_FILE}")
        return None
    except json.JSONDecodeError as e:
        log_print(f"Error: Invalid JSON in config file: {e}")
        return None

def remove_raw_from_query(sql_query: str) -> str:
    """
    Remove all mentions of the word 'RAW' from the SQL query.
    Handles various cases like 'SELECT RAW', 'raw', 'Raw', etc.
    """
    # Remove RAW keyword (case-insensitive) with proper spacing
    # Pattern: SELECT RAW or SELECT   RAW (with multiple spaces)
    modified_query = re.sub(r'\bRAW\b', '', sql_query, flags=re.IGNORECASE)
    
    # Clean up any double spaces that might have been created
    modified_query = re.sub(r'\s+', ' ', modified_query)
    
    return modified_query.strip()

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
    payload = {
        "statement": sql_query,
    }
    
    headers = {
        "Content-Type": "application/json",
        "Authorization": requests.auth._basic_auth_str(couchbase_user, couchbase_pass)
    }
    
    response = None
    try:
        response = requests.post(query_endpoint, json=payload, headers=headers, timeout=60)
        response.raise_for_status()
        data = response.json()
        return data.get("results", [])
    except requests.exceptions.RequestException as e:
        log_print(f"  ✗ Error executing SQL: {e}")
        if response:
            try:
                error_data = response.json()
                log_print(f"  Error details: {error_data}")
            except:
                log_print(f"  Response text: {response.text[:500]}")
        return None

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
        # Empty result - create empty CSV file
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
        log_print(f"  ✗ Error converting to CSV: {e}")
        return False

def process_all_queries():
    """Main function to process all SQL queries."""
    
    # Initialize log
    init_log_file()
    
    # Load configuration
    config = load_config()
    if not config:
        log_print("Failed to load configuration. Exiting.")
        close_log_file()
        return
    
    # Extract Couchbase connection details
    couchbase_config = config.get("couchbase", {})
    couchbase_user = couchbase_config.get("username")
    couchbase_pass = couchbase_config.get("password")
    query_endpoint = resolve_query_endpoint_from_config(couchbase_config, couchbase_user, couchbase_pass)
    
    if not all([couchbase_user, couchbase_pass, query_endpoint]):
        log_print("Error: Missing required Couchbase configuration")
        close_log_file()
        return
    
    # Ensure output directory exists
    OUTPUT_CSV_DIR.mkdir(parents=True, exist_ok=True)
    
    # Get all SQL files
    sql_files = sorted(INPUT_SQL_DIR.glob("*.sql"))
    
    if not sql_files:
        log_print(f"No SQL files found in {INPUT_SQL_DIR}")
        close_log_file()
        return
    
    log_print(f"Found {len(sql_files)} SQL files to process")
    log_print(f"Input directory: {INPUT_SQL_DIR}")
    log_print(f"Output directory: {OUTPUT_CSV_DIR}")
    log_print("="*80)
    
    # Statistics
    total_files = len(sql_files)
    successful = 0
    failed = 0
    
    # Process each SQL file
    for idx, sql_file in enumerate(sql_files, 1):
        query_num = sql_file.stem  # Get filename without extension
        log_print(f"\n[{idx}/{total_files}] Processing query {query_num}")
        
        # Read original SQL
        try:
            with open(sql_file, 'r') as f:
                original_sql = f.read()
        except Exception as e:
            log_print(f"  ✗ Error reading file: {e}")
            failed += 1
            continue
        
        # Remove RAW keyword
        modified_sql = remove_raw_from_query(original_sql)
        
        # Check if anything was changed
        if original_sql != modified_sql:
            log_print(f"  → Removed RAW keyword")
        else:
            log_print(f"  → No RAW keyword found")
        
        # Execute the modified query
        log_print(f"  → Executing query...")
        results = exec_sql(modified_sql, couchbase_user, couchbase_pass, query_endpoint)
        
        if results is None:
            log_print(f"  ✗ Failed to execute query")
            failed += 1
            continue
        
        # Save results as CSV
        output_csv = OUTPUT_CSV_DIR / f"{query_num}.csv"
        log_print(f"  → Saving results to {output_csv.name}")
        
        if json_to_csv(results, output_csv):
            log_print(f"  ✓ Success! ({len(results)} rows)")
            successful += 1
        else:
            log_print(f"  ✗ Failed to save CSV")
            failed += 1
        
        # Small delay to avoid overwhelming the server
        time.sleep(0.1)
    
    # Print summary
    log_print("\n" + "="*80)
    log_print("SUMMARY")
    log_print("="*80)
    log_print(f"Total queries: {total_files}")
    log_print(f"Successful: {successful}")
    log_print(f"Failed: {failed}")
    log_print(f"Success rate: {successful/total_files*100:.2f}%")
    log_print(f"\nOutput saved to: {OUTPUT_CSV_DIR}")
    log_print(f"Log saved to: {LOG_FILE}")
    
    close_log_file()

if __name__ == "__main__":
    process_all_queries()
