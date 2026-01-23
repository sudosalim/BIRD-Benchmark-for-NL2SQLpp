from pathlib import Path

folder_path = Path('comparingResults/sql_query_results')  # Replace with your folder path
num_files = len([f for f in folder_path.iterdir() if f.is_file()])
print(f"Number of files: {num_files}")