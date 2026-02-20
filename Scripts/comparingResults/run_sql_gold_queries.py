import json
import sqlite3
import csv
import os
from pathlib import Path

SCRIPT_DIR= Path(__file__).parent
DATA_DIR= SCRIPT_DIR.parent.parent / "mini_dev_data"

# Load the questions and queries
with open(DATA_DIR / "mini_dev_sqlite.json", 'r') as f:
    questions = json.load(f)

# Create output directory for results
output_dir = '2sql_query_results'
os.makedirs(output_dir, exist_ok=True)

# Process each question
for item in questions:
    question_id = item['question_id']
    db_id = item['db_id']
    sql_query = item['SQL']
    
    # Construct database path
    db_path = DATA_DIR / f'dev_databases/{db_id}/{db_id}.sqlite'
    
    # Check if database exists
    if not os.path.exists(db_path):
        print(f"Warning: Database not found for question {question_id}: {db_path}")
        continue
    
    try:
        # Connect to database
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Execute query
        cursor.execute(sql_query)
        results = cursor.fetchall()
        
        # Get column names
        column_names = [description[0] for description in cursor.description] if cursor.description else []
        
        # Save to CSV
        csv_filename = f'{output_dir}/question_{question_id}_{db_id}.csv'
        with open(csv_filename, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile)
            
            # Write header
            if column_names:
                writer.writerow(column_names)
            
            # Write results
            writer.writerows(results)
        
        print(f"✓ Question {question_id} ({db_id}): {len(results)} rows saved to {csv_filename}")
        
        # Close connection
        conn.close()
        
    except Exception as e:
        print(f"✗ Error processing question {question_id} ({db_id}): {str(e)}")

print(f"\nAll results saved to '{output_dir}/' directory")