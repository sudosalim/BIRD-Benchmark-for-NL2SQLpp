import sqlite3
import json
# from datasets import load_dataset

with open(
    "/Users/aayush.fabwani/Documents/Projects/Benchmark/Work/mini_dev/mini_dev_data/mini_dev_sqlite.json",
    "r"
) as f:
    dataset = json.load(f)

questions = [ex["question"] for ex in dataset]

print("Number of questions:", len(questions))
print("First question:", questions[0])