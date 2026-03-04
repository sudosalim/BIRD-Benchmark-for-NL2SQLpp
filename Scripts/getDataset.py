# from datasets import load_dataset

# dataset = load_dataset("birdsql/bird_mini_dev")

# print(dataset)
# print(dataset.keys())

# sqlite_split = dataset["mini_dev_sqlite"]
# print("Number of examples:", len(sqlite_split))
# print("First example:")
# print(sqlite_split[0])


from datasets import load_dataset
import os

dataset = load_dataset("birdsql/bird_mini_dev")
ex = dataset["mini_dev_sqlite"][0]

db_id = ex["db_id"]
db_path = f"../mini_dev_data/dev_databases/{db_id}/{db_id}.sqlite"

print("DB ID:", db_id)
print("DB exists:", os.path.exists(db_path))
