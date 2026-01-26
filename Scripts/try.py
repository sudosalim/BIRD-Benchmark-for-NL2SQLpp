import json
from pathlib import Path

SCRIPT_DIR = Path(__file__).parent
CONFIG_FILE = SCRIPT_DIR.parent / "IQ_config.json"
OUTPUT_DIR = SCRIPT_DIR / "couchbase_iq_results"
OUTPUT_SQL_DIR = OUTPUT_DIR / "sql_outputs"
OUTPUT_CSV_DIR = OUTPUT_DIR / "csv_outputs"
JSON_OUTPUT_DIR = SCRIPT_DIR / "sqlLiteToJson" / "json_outputs"

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
        if isinstance(table_data[0], dict):
            fields = list(table_data[0].keys())
        
        collections_with_fields[table_name] = fields
    
    return collections_with_fields

if __name__ == "__main__":
    bucket = "student_club"
    scope = "bird"
    
    collections = get_all_collections_in_scope(bucket, scope)
    for collection, fields in collections.items():
        print(f"Collection: {collection}, Fields: {fields}")
