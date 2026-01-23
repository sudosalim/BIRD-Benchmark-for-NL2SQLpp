import json
from pathlib import Path
from typing import List
import re

SCRIPT_DIR = Path(__file__).parent
JSON_OUTPUT_DIR = SCRIPT_DIR / "sqlLiteToJson" / "json_outputs"

def reduce_keyspace_via_collection(statements: List[str], keyspaces: List[str]) -> List[str]:
    relevant_collections=[]
    for s in statements:
        s=s.lower()
        q_words=set(re.findall(r"\b\w+\b", s))
        for w in keyspaces:
            if w.lower() in q_words:
                relevant_collections.append(w)
    return relevant_collections

def main():
    json_path=Path(JSON_OUTPUT_DIR)/f"debit_card_specializing.json"
    with open(json_path,"r") as f:
        db_data=json.load(f)
    scope_names=set()
    for table_name in db_data.keys():
        scope_names.add(table_name)
    scope_names.discard("sqlite_sequence")
    statements=[
         "In 2012, who had the least consumption in LAM?",
            "Year 2012 can be presented as Between 201201 And 201212; The first 4 strings of the Date values in the yearmonth table can represent year.",
           
    ]
    scope_names=reduce_keyspace_via_collection(statements,list(scope_names))
    print(scope_names)

if __name__ == "__main__":
    main()
