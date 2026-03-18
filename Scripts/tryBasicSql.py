import sqlite3
import json
# from datasets import load_dataset

with open(
    "/Users/aayush.fabwani/Documents/Projects/Benchmark/Work/mini_dev/mini_dev_data/mini_dev_sqlite.json",
    "r"
) as f:
    dataset = json.load(f)

# dataset = load_dataset("birdsql/bird_mini_dev")
ex = dataset[0]

db_path = f"/Users/aayush.fabwani/Documents/Projects/Benchmark/Work/mini_dev/mini_dev_data/dev_databases/{ex['db_id']}/{ex['db_id']}.sqlite"

conn = sqlite3.connect(db_path)
cur = conn.cursor()

cur.execute(ex["SQL"])
rows = cur.fetchall()

print("Returned rows:", rows[:5])
