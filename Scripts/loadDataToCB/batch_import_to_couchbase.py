import sys
from pathlib import Path
from import_to_couchbase import import_json_to_couchbase

def batch_import_json_files(
    json_directory,
    cluster_url="couchbase://127.0.0.1",
    username="Administrator",
    password="password",
    scope_name="bird",
    use_separate_collections=True,
    ram_quota_mb=600,
    skip_existing=True
):
    """
    Import all JSON files from a directory into Couchbase
    
    Args:
        json_directory: Path to directory containing JSON files
        cluster_url: Couchbase cluster connection string
        username: Couchbase username
        password: Couchbase password
        scope_name: Target scope name (defaults to spider2)
        use_separate_collections: If True, create a collection per table
        ram_quota_mb: RAM quota for bucket creation (default 600 MB)
        skip_existing: If True, skip files that have already been imported
    """
    
    json_dir = Path(json_directory)
    
    if not json_dir.exists():
        print(f"Error: Directory not found: {json_directory}")
        return False
    
    # Find all JSON files
    json_files = sorted(json_dir.glob("*.json"))
    
    if not json_files:
        print(f"No JSON files found in {json_directory}")
        return False
    
    print(f"\n{'='*70}")
    print(f"Batch Import from: {json_dir}")
    print(f"Found {len(json_files)} JSON files")
    print(f"{'='*70}\n")
    
    # Display all files
    print("Files to import:")
    for idx, json_file in enumerate(json_files, 1):
        file_size = json_file.stat().st_size / (1024 * 1024)  # Size in MB
        print(f"  {idx:2d}. {json_file.name:40s} ({file_size:8.1f} MB)")
    print()
    
    # Process each file
    success_count = 0
    fail_count = 0
    skip_count = 0
    
    for idx, json_file in enumerate(json_files, 1):
        bucket_name = json_file.stem
        
        print(f"\n{'='*70}")
        print(f"Processing file {idx}/{len(json_files)}: {json_file.name}")
        print(f"{'='*70}")
        
        try:
            success = import_json_to_couchbase(
                json_file_path=str(json_file),
                cluster_url=cluster_url,
                username=username,
                password=password,
                bucket_name=bucket_name,
                scope_name=scope_name,
                use_separate_collections=use_separate_collections,
                ram_quota_mb=ram_quota_mb
            )
            
            if success:
                success_count += 1
                print(f"✓ Successfully imported {json_file.name}")
            else:
                fail_count += 1
                print(f"✗ Failed to import {json_file.name}")
                
        except Exception as e:
            fail_count += 1
            print(f"✗ Error importing {json_file.name}: {e}")
        
        print()
    
    # Summary
    print(f"\n{'='*70}")
    print("BATCH IMPORT SUMMARY")
    print(f"{'='*70}")
    print(f"Total files:      {len(json_files)}")
    print(f"✓ Successful:     {success_count}")
    print(f"✗ Failed:         {fail_count}")
    print(f"⊘ Skipped:        {skip_count}")
    print(f"{'='*70}\n")
    
    return fail_count == 0

def main():
    CLUSTER_URL = "couchbase://127.0.0.1"
    USERNAME = "Administrator"
    PASSWORD = "password"

    #add path to the folder where you have stored the json files (after converting from sqlite) that you want to import to couchbase
    JSON_DIRECTORY=""

    SCOPE_NAME = "bird"
    USE_SEPARATE_COLLECTIONS = True  # Create a collection per table
    RAM_QUOTA_MB = 600  # RAM quota for each bucket
    
    print("\n" + "="*70)
    print("Couchbase Batch JSON Importer")
    print("="*70)
    
    success = batch_import_json_files(
        json_directory=JSON_DIRECTORY,
        cluster_url=CLUSTER_URL,
        username=USERNAME,
        password=PASSWORD,
        scope_name=SCOPE_NAME,
        use_separate_collections=USE_SEPARATE_COLLECTIONS,
        ram_quota_mb=RAM_QUOTA_MB
    )
    
    if success:
        print("All imports completed successfully!")
        sys.exit(0)
    else:
        print("Some imports failed. Check the output above for details.")
        sys.exit(1)


if __name__ == "__main__":
    main()

