
import sqlite3
import json
import os
from pathlib import Path
from typing import Dict, List, Any

def get_all_tables(conn: sqlite3.Connection) -> List[str]:
    """Get all table names from the SQLite database."""
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = [row[0] for row in cursor.fetchall()]
    return tables


def get_table_data(conn: sqlite3.Connection, table_name: str) -> List[Dict[str, Any]]:
    """Extract all data from a specific table."""
    cursor = conn.cursor()
    cursor.execute(f"SELECT * FROM {table_name}")
    
    # Get column names
    column_names = [description[0] for description in cursor.description]
    
    # Fetch all rows and convert to list of dictionaries
    rows = cursor.fetchall()
    table_data = []
    for row in rows:
        row_dict = {}
        for i, value in enumerate(row):
            # Convert any non-JSON serializable types
            if isinstance(value, bytes):
                row_dict[column_names[i]] = value.decode('utf-8', errors='ignore')
            else:
                row_dict[column_names[i]] = value
        table_data.append(row_dict)
    
    return table_data


def export_sqlite_to_json(sqlite_path: str, output_path: str) -> None:
    """
    Export entire SQLite database to JSON format.
    
    Args:
        sqlite_path: Path to the SQLite database file
        output_path: Path where the JSON file will be saved
    """
    print(f"Processing: {sqlite_path}")
    
    try:
        # Connect to the database
        conn = sqlite3.connect(sqlite_path)
        
        # Get all tables
        tables = get_all_tables(conn)
        print(f"  Found {len(tables)} tables")
        
        # Extract data from all tables
        database_data = {}
        for table in tables:
            try:
                table_data = get_table_data(conn, table)
                database_data[table] = table_data
                print(f"    - {table}: {len(table_data)} rows")
            except Exception as e:
                print(f"    - Error reading table {table}: {e}")
                database_data[table] = {
                    "error": str(e)
                }
        
        # Close connection
        conn.close()
        
        # Save to JSON
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(database_data, f, indent=2, ensure_ascii=False)
        
        print(f"  ✓ Saved to: {output_path}\n")
        
    except Exception as e:
        print(f"  ✗ Error processing {sqlite_path}: {e}\n")


def main():
    # Specify the directory where your SQLite files are located and the output directory for JSON files
    sqlite_dir=Path("mini_dev/mini_dev_data/dev_databases")
    output_dir=Path("mini_dev/Scripts/sqlLiteToJson/json_outputs")

    # Create output directory if it doesn't exist
    output_dir.mkdir(exist_ok=True)
    
    # Get all .sqlite files
    sqlite_files = list(sqlite_dir.glob("**/*.sqlite"))
    
    if not sqlite_files:
        print(f"No SQLite files found in {sqlite_dir}")
        return
    
    print(f"Found {len(sqlite_files)} SQLite files to process\n")
    print("=" * 60)
    
    # Process each SQLite file
    for sqlite_file in sorted(sqlite_files):
        # Create output filename (same as sqlite file but with .json extension)
        output_file = output_dir / f"{sqlite_file.stem}.json"
        
        # Export to JSON
        export_sqlite_to_json(str(sqlite_file), str(output_file))
    
    print("=" * 60)
    print(f"\n✓ All files processed!")
    print(f"Output directory: {output_dir}")
    print(f"Total files created: {len(list(output_dir.glob('*.json')))}")


if __name__ == "__main__":
    main()
