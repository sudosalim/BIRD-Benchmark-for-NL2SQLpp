"""
Import JSON files exported from SQLite into Couchbase Server
Each JSON file contains multiple tables with their data
Creates bucket and collections automatically
"""


import json
import sys
import requests
from pathlib import Path
from datetime import timedelta
import time
from urllib.parse import urlparse

SCRIPT_ROOT = Path(__file__).resolve().parents[2]
IQ_CONFIG_FILE = SCRIPT_ROOT / "IQ_config.json"

try:
    from couchbase.cluster import Cluster
    from couchbase.auth import PasswordAuthenticator
    from couchbase.options import ClusterOptions
    from couchbase.exceptions import CouchbaseException
    from couchbase.management.buckets import CreateBucketSettings, BucketType
    from couchbase.management.collections import CollectionSpec
except ImportError:
    print("Error: Couchbase Python SDK not installed.")
    print("Install it with: pip install couchbase")
    sys.exit(1)


def load_couchbase_config_from_iq():
    """Load Couchbase host/user/password (and optional cert_path) from IQ_config.json."""
    defaults = {
        "cluster_url": "127.0.0.1",
        "username": "Administrator",
        "password": "password",
        "cert_path": None
    }

    try:
        if IQ_CONFIG_FILE.exists():
            with open(IQ_CONFIG_FILE, "r") as f:
                cfg = json.load(f)
            cb_cfg = cfg.get("couchbase", {})
            defaults["cluster_url"] = (
                cb_cfg.get("cb_endpoint")
                or cb_cfg.get("host")
                or cb_cfg.get("connection_string")
                or defaults["cluster_url"]
            )
            defaults["username"] = cb_cfg.get("username", defaults["username"])
            defaults["password"] = cb_cfg.get("password", defaults["password"])
            defaults["cert_path"] = cb_cfg.get("cert_path", defaults["cert_path"])
    except Exception as e:
        print(f"Warning: Could not read {IQ_CONFIG_FILE}: {e}")

    return defaults

def _extract_host_and_port(cluster_url: str):
    """Extract host and optional port from user input.

    Accepts values like:
      - 127.0.0.1
      - 127.0.0.1:12000
      - couchbase://127.0.0.1:12000
      - http://127.0.0.1:9000
    """
    raw = cluster_url.strip()
    if "://" not in raw:
        raw = f"couchbase://{raw}"
    parsed = urlparse(raw)
    return parsed.hostname, parsed.port


def _discover_services(host: str, username: str, password: str, mgmt_ports=None):
    """Discover service ports from nodeServices on common mgmt ports."""
    if mgmt_ports is None:
        mgmt_ports = [9000, 8091]

    for mgmt_port in mgmt_ports:
        endpoint = f"http://{host}:{mgmt_port}/pools/default/nodeServices"
        try:
            response = requests.get(endpoint, auth=(username, password), timeout=3)
            response.raise_for_status()
            payload = response.json()
            nodes = payload.get("nodesExt", [])
            if not nodes:
                continue
            node = next((n for n in nodes if n.get("thisNode")), nodes[0])
            services = node.get("services", {})
            if services:
                return services, mgmt_port
        except requests.exceptions.RequestException:
            continue
    return {}, None


def is_tls_connection(cluster_url: str) -> bool:
    """True for couchbases:// connection strings, e.g. a Capella cluster."""
    return cluster_url.strip().startswith("couchbases://")


def resolve_cluster_endpoints(cluster_url: str, username: str, password: str):
    """Resolve SDK bootstrap URL and Query endpoint from input host/URL.

    A couchbases:// connection string (Capella) is used as given: its management
    ports aren't reachable, and the Query service listens on 18093 over TLS.
    """
    host, input_port = _extract_host_and_port(cluster_url)
    if not host:
        raise ValueError(f"Unable to parse host from '{cluster_url}'")

    if is_tls_connection(cluster_url):
        return cluster_url.strip(), f"https://{host}:18093/query/service"

    mgmt_ports = []
    if input_port:
        mgmt_ports.append(input_port)
    for p in (9000, 8091):
        if p not in mgmt_ports:
            mgmt_ports.append(p)

    services, _ = _discover_services(host, username, password, mgmt_ports)

    mgmt_port_set = set(mgmt_ports)
    input_is_mgmt_port = input_port in mgmt_port_set

    kv_port = services.get("kv") or (input_port if input_port and not input_is_mgmt_port else None) or 11210
    n1ql_port = services.get("n1ql") or 8093

    sdk_cluster_url = f"couchbase://{host}:{kv_port}"
    query_endpoint = f"http://{host}:{n1ql_port}/query/service"
    return sdk_cluster_url, query_endpoint


def exec_sql(sql_query: str, query_endpoint: str, username: str, password: str, cert_path=None) -> bool:
    """
    Execute SQL++ statement on Couchbase Query Service

    Args:
        sql_query: SQL++ query to execute
        query_endpoint: Query service endpoint URL
        username: Couchbase username
        password: Couchbase password
        cert_path: CA certificate to verify an https endpoint with (None: system CAs)

    Returns:
        True if successful, False otherwise
    """
    payload = {"statement": sql_query}
    headers = {"Content-Type": "application/json"}

    response = None
    try:
        response = requests.post(
            query_endpoint,
            json=payload,
            headers=headers,
            auth=(username, password),
            timeout=30,
            verify=cert_path or True
        )
        response.raise_for_status()
        return True
    except requests.exceptions.RequestException as e:
        print(f"  SQL Error: {e}")
        if response is not None:
            try:
                error_data = response.json()
                # Check if error is because entity already exists - that's OK
                if "already exists" in str(error_data):
                    return True
                print(f"  Details: {error_data}")
            except:
                print(f"  Response: {response.text}")
        return False


def create_bucket_if_not_exists(cluster, bucket_name: str, ram_quota_mb:int=256):
    try:
        bucket_mgr=cluster.buckets()
        try:
            bucket_mgr.get_bucket(bucket_name)
            print(f"Bucket {bucket_name} already exists.")
            return True
        except CouchbaseException:
            print(f"Creating bucket {bucket_name}...")
            bucket_settings=CreateBucketSettings(
                name=bucket_name,
                ram_quota_mb=ram_quota_mb,
                bucket_type=BucketType.COUCHBASE
            )

            bucket_mgr.create_bucket(bucket_settings)

            print(f"Waiting for bucket {bucket_name} to be ready...")
            time.sleep(5)  # Wait for bucket to be ready
            return True
    except Exception as e:
        print(f"Error creating/accessing bucket {bucket_name}: {e}")
        return False

def create_scope_if_not_exists(
    cluster,
    bucket_name: str,
    scope_name: str,
    username: str,
    password: str,
    query_endpoint: str,
    cert_path=None
):
    if scope_name == "_default":
        print("Using default scope, no need to create.")
        return True
    try:
        bucket=cluster.bucket(bucket_name)
        coll_mgr=bucket.collections()
        scopes=coll_mgr.get_all_scopes()

        for scope in scopes:
            if scope.name==scope_name:
                print(f"Scope {scope_name} already exists.")
                return True
        
        #CREATE SCOPE USING SQL++
        print(f"Creating scope {scope_name}...")
        create_scope_sqlpp=f"CREATE SCOPE `{bucket_name}`.`{scope_name}` IF NOT EXISTS"

        if exec_sql(create_scope_sqlpp, query_endpoint, username, password, cert_path):
            print(f"  ✓ Scope '{scope_name}' created successfully")
            time.sleep(1)  # Wait for scope to be ready
            return True
        else:
            # Fallback: try using SDK
            try:
                coll_mgr.create_scope(scope_name)
                print(f"  ✓ Scope '{scope_name}' created successfully (via SDK)")
                time.sleep(1)
                return True
            except Exception as e:
                if "already exists" in str(e).lower():
                    print(f"  ✓ Scope '{scope_name}' already exists")
                    return True
                print(f"  ✗ Error creating scope via SDK: {e}")
                return False
            
    except Exception as e:
        if "already exists" in str(e).lower():
            print(f"  ✓ Scope '{scope_name}' already exists")
            return True
        print(f"  ✗ Error creating scope: {e}")
        return False

def create_collection_if_not_exists(
    cluster,
    bucket_name: str,
    scope_name: str,
    collection_name: str,
    username: str,
    password: str,
    query_endpoint: str,
    cert_path=None
):
    """
    Create a collection if it doesn't exist
    
    Args:
        cluster: Couchbase cluster object
        bucket_name: Bucket name
        scope_name: Scope name
        collection_name: Collection name to create
        username: Couchbase username
        password: Couchbase password
        query_endpoint: Query service endpoint URL
        
    Returns:
        True if collection exists or was created, False otherwise
    """
    try:
        
        # Create collection using SQL++
        if scope_name == "_default" and collection_name == "_default":
            # _default._default always exists
            return True
        
        create_collection_sql = f'CREATE COLLECTION `{bucket_name}`.`{scope_name}`.`{collection_name}` IF NOT EXISTS'
        
        if exec_sql(create_collection_sql, query_endpoint, username, password, cert_path):
            print(f"  ✓ Collection '{collection_name}' ready in scope '{scope_name}'")
            time.sleep(0.5)  # Brief delay for collection to be ready
            return True
        else:
            # Fallback to SDK method
            try:
                bucket = cluster.bucket(bucket_name)
                collection_manager = bucket.collections()
                
                # Check if collection exists
                scopes = collection_manager.get_all_scopes()
                for scope in scopes:
                    if scope.name == scope_name:
                        for collection in scope.collections:
                            if collection.name == collection_name:
                                print(f"  ✓ Collection '{collection_name}' already exists")
                                return True
                
                # Create collection
                collection_spec = CollectionSpec(collection_name, scope_name)
                collection_manager.create_collection(collection_spec)
                
                print(f"  ✓ Collection '{collection_name}' created in scope '{scope_name}'")
                time.sleep(1)
                return True
                
            except Exception as e:
                if "already exists" in str(e).lower():
                    return True
                print(f"  ✗ Error creating collection via SDK: {e}")
                return False
            
    except Exception as e:
        if "already exists" in str(e).lower():
            return True
        print(f"  ✗ Error creating collection: {e}")
        return False


UPSERT_BATCH_SIZE = 1000


def upsert_in_batches(collection, keyed_docs, total: int, indent: str = ""):
    """
    Upsert (key, doc) pairs UPSERT_BATCH_SIZE at a time with upsert_multi, so a
    remote cluster (e.g. Capella) costs one round trip per batch, not per document.
    A key that repeats within a batch keeps its last document, as sequential
    upserts would.

    Returns:
        (success_count, error_count)
    """
    success_count = 0
    error_count = 0
    done = 0
    batch = {}

    def flush():
        nonlocal success_count, error_count
        if not batch:
            return
        try:
            result = collection.upsert_multi(batch)
            failed = result.exceptions or {}
        except CouchbaseException as e:
            failed = {key: e for key in batch}
        for key, e in list(failed.items()):
            error_count += 1
            if error_count <= 5:  # Only print first 5 errors
                print(f"{indent}Error inserting document {key}: {e}")
        success_count += len(batch) - len(failed)
        batch.clear()

    for key, doc in keyed_docs:
        batch[key] = doc
        done += 1
        if len(batch) >= UPSERT_BATCH_SIZE:
            flush()
            # Progress indicator
            print(f"{indent}Progress: {done}/{total} documents...")
    flush()
    return success_count, error_count


def import_json_to_couchbase(
    json_file_path,
    cluster_url=None,
    username=None,
    password=None,
    bucket_name=None,
    scope_name="bird",
    use_separate_collections=True,
    ram_quota_mb=600,
    cert_path=None
):
    """
    Import a JSON file into Couchbase

    Args:
        json_file_path: Path to the JSON file
        cluster_url: Couchbase host or URL (e.g. 127.0.0.1, couchbase://127.0.0.1:12000,
            or couchbases://cb.<id>.cloud.couchbase.com for Capella)
        username: Couchbase username
        password: Couchbase password
        bucket_name: Target bucket name (defaults to filename without extension)
        scope_name: Target scope name (defaults to spider2)
        use_separate_collections: If True, create a collection per table. If False, use _default collection
        ram_quota_mb: RAM quota for bucket creation (default 256 MB)
        cert_path: CA certificate for TLS connections (e.g. the cluster's root CA from Capella)

    On Capella (couchbases://) the bucket must already exist: database users
    can't create buckets, so create it in the Capella UI or API first.
    """

    cfg = load_couchbase_config_from_iq()
    cluster_url = cluster_url or cfg["cluster_url"]
    username = username or cfg["username"]
    password = password or cfg["password"]
    cert_path = cert_path or cfg["cert_path"]

    json_path=Path(json_file_path)

    if not json_path.exists():
        print(f"Error: File not found: {json_file_path}")
        return False
    
    # Use filename as bucket name if not specified
    if bucket_name is None:
        bucket_name = json_path.stem

    print(f"\n{'='*70}")
    print(f"Importing: {json_path.name}")
    print(f"Target Bucket: {bucket_name}")
    print(f"Target Scope: {scope_name}")
    print(f"Use Separate Collections: {use_separate_collections}")
    print(f"{'='*70}\n")

    # Load the JSON file
    print("Loading JSON file...")
    try:
        with open(json_path, 'r') as f:
            data = json.load(f)
    except json.JSONDecodeError as e:
        print(f"Error: Invalid JSON format: {e}")
        return False
    except Exception as e:
        print(f"Error reading file: {e}")
        return False
    
    # Connect to Couchbase
    print(f"Connecting to Couchbase at {cluster_url}...")
    try:
        sdk_cluster_url, query_endpoint = resolve_cluster_endpoints(cluster_url, username, password)
        print(f"Resolved SDK endpoint: {sdk_cluster_url}")
        print(f"Resolved Query endpoint: {query_endpoint}")

        if cert_path:
            auth = PasswordAuthenticator(username, password, cert_path=str(Path(cert_path).resolve()))
        else:
            auth = PasswordAuthenticator(username, password)
        print("SS: Auth object: ",auth)
        options = ClusterOptions(auth)
        print("SS: Options object: ",options)
        options.apply_profile("wan_development")  # Use for localhost
        print("SS: Options object after apply_profile: ",options)
        cluster = Cluster(sdk_cluster_url, options)
        print("SS: Cluster object: ",cluster)
        # Wait until cluster is ready
        cluster.wait_until_ready(timedelta(seconds=30))

        print(f"✓ Connected to Couchbase cluster\n")

        if is_tls_connection(cluster_url):
            # Capella: database users can't manage buckets, so it must exist already
            print(f"Using existing bucket '{bucket_name}' (create it in Capella first)")
            cluster.bucket(bucket_name)
        else:
            # Create bucket if it doesn't exist
            print(f"Setting up bucket '{bucket_name}'...")
            if not create_bucket_if_not_exists(cluster, bucket_name, ram_quota_mb):
                print(f"Failed to create/access bucket '{bucket_name}'")
                return False

        # Create scope if it doesn't exist
        if scope_name != "_default":
            print(f"Setting up scope '{scope_name}'...")
            if not create_scope_if_not_exists(cluster, bucket_name, scope_name, username, password, query_endpoint, cert_path):
                print(f"Failed to create/access scope '{scope_name}'")
                return False
            print()

    except CouchbaseException as e:
        print(f"Error connecting to Couchbase: {e}")
        print(f"\nMake sure:")
        print(f"  1. Couchbase Server is running")
        print(f"  2. Credentials are correct (username: {username})")
        return False
    
    total_docs=0

    # Check if data is organized by tables
    if isinstance(data, dict):
        # Data organized as {table_name: [rows]} OR {table_name: {row_count, data}}
        print(f"Found {len(data)} tables in JSON file\n")
        
        for table_name, table_data in data.items():
            # Handle two formats:
            # Format 1: {table_name: [rows]} - table_data is a list of rows directly
            # Format 2: {table_name: {"data": [rows], "row_count": N}} - table_data is a dict
            
            if isinstance(table_data, list):
                # Format 1: table_data is the list of rows directly
                rows = table_data
            elif isinstance(table_data, dict) and 'data' in table_data:
                # Format 2: table_data has 'data' key with rows
                rows = table_data['data']
            else:
                print(f"  ⚠ Skipping '{table_name}': unexpected format (type: {type(table_data)})")
                if isinstance(table_data, dict):
                    print(f"    Available keys: {list(table_data.keys())}")
                continue
            
            row_count = len(rows)
            
            print(f"Processing table: {table_name} ({row_count} rows)")
            
            # Determine collection name
            if use_separate_collections:
                collection_name = table_name
                # Create collection for this table
                print(f"  Setting up collection '{collection_name}'...")
                if not create_collection_if_not_exists(
                    cluster, bucket_name, scope_name, collection_name,
                    username, password, query_endpoint, cert_path
                ):
                    print(f"  ✗ Failed to create collection '{collection_name}', skipping table")
                    continue
            else:
                collection_name = "_default"
            
            # Get reference to collection
            try:
                bucket = cluster.bucket(bucket_name)
                # Verify scope exists
                collection_manager = bucket.collections()
                scopes = collection_manager.get_all_scopes()
                scope_exists = any(s.name == scope_name for s in scopes)
                if not scope_exists:
                    print(f"  ✗ Error: Scope '{scope_name}' does not exist in bucket '{bucket_name}'")
                    print(f"  Available scopes: {[s.name for s in scopes]}")
                    continue
                
                scope = bucket.scope(scope_name)
                collection = scope.collection(collection_name)
            except Exception as e:
                print(f"  ✗ Error accessing collection '{collection_name}' in scope '{scope_name}': {e}")
                import traceback
                traceback.print_exc()
                continue
            
            print(f"  Importing to collection '{collection_name}'...")

            def keyed_rows():
                for idx, row in enumerate(rows, 1):
                    # Generate key: tablename_id or tablename_idx
                    # Try to use an 'id' field if available, otherwise use index
                    if 'id' in row:
                        key = f"{table_name}_{row['id']}"
                    elif 'businessentityid' in row:
                        key = f"{table_name}_{row['businessentityid']}"
                    else:
                        # Use index as fallback
                        key = f"{table_name}_{idx}"

                    # Add metadata
                    yield key, {
                        **row,
                        '_table': table_name,
                        '_source': json_path.name
                    }

            success_count, error_count = upsert_in_batches(collection, keyed_rows(), row_count, "    ")

            print(f"  ✓ Completed: {success_count} successful, {error_count} errors\n")
            total_docs += success_count
                
    elif isinstance(data, list):
        # Data is a simple array of documents
        print(f"Importing {len(data)} documents...")
        
        # Use default collection for list data
        collection_name = "_default"
        
        try:
            bucket = cluster.bucket(bucket_name)
            scope = bucket.scope(scope_name)
            collection = scope.collection(collection_name)
        except Exception as e:
            print(f"  ✗ Error accessing collection: {e}")
            return False
        
        def keyed_docs():
            for idx, doc in enumerate(data, 1):
                # Generate key
                if 'id' in doc:
                    key = f"doc_{doc['id']}"
                else:
                    key = f"doc_{idx}"

                # Add metadata
                doc['_source'] = json_path.name
                yield key, doc

        success_count, error_count = upsert_in_batches(collection, keyed_docs(), len(data), "  ")

        print(f"✓ Completed: {success_count} successful, {error_count} errors")
        total_docs = success_count
    
    print(f"\n{'='*70}")
    print(f"Import completed!")
    print(f"Total documents imported: {total_docs}")
    print(f"{'='*70}\n")
    
    return True


def main():
    cfg = load_couchbase_config_from_iq()

    CLUSTER_URL = cfg["cluster_url"]
    USERNAME = cfg["username"]
    PASSWORD = cfg["password"]

    JSON_FILE="data.json"  # Path to your JSON file

    BUCKET_NAME = "AdventureWorks"  # Will use filename if None
    SCOPE_NAME = "spider2"
    USE_SEPARATE_COLLECTIONS = True  # Create a collection per table
    RAM_QUOTA_MB = 600  # RAM quota for bucket
    
    print("\n" + "="*70)
    print("Couchbase JSON Importer with Auto Collection Creation")
    print("="*70)

    success = import_json_to_couchbase(
        json_file_path=JSON_FILE,
        cluster_url=CLUSTER_URL,
        username=USERNAME,
        password=PASSWORD,
        bucket_name=BUCKET_NAME,
        scope_name=SCOPE_NAME,
        use_separate_collections=USE_SEPARATE_COLLECTIONS,
        ram_quota_mb=RAM_QUOTA_MB
    )

    if success:
        print("Import successful!")
        sys.exit(0)
    else:
        print("Import failed!")
        sys.exit(1)


if __name__ == "__main__":
    main()