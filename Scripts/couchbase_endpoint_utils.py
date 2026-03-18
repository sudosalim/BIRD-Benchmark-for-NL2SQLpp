from urllib.parse import urlparse

import requests


DEFAULT_CB_ENDPOINT = "127.0.0.1:9000"


def extract_host_and_port(connection_value: str):
    """Extract host and optional port from host or URL values."""
    if not connection_value:
        return "localhost", None

    raw = str(connection_value).strip()
    if "://" not in raw:
        raw = f"http://{raw}"

    parsed = urlparse(raw)
    return parsed.hostname or "localhost", parsed.port


def resolve_query_endpoint_from_config(couchbase_config, username: str, password: str) -> str:
    """Resolve Query endpoint from `cb_endpoint` (host:management-port)."""
    connection_value = (
        couchbase_config.get("cb_endpoint")
        or couchbase_config.get("host")
        or couchbase_config.get("connection_string")
        or DEFAULT_CB_ENDPOINT
    )
    host, mgmt_port_from_input = extract_host_and_port(connection_value)

    mgmt_ports = []
    if mgmt_port_from_input:
        mgmt_ports.append(mgmt_port_from_input)
    for port in (9000, 8091):
        if port not in mgmt_ports:
            mgmt_ports.append(port)

    for mgmt_port in mgmt_ports:
        node_services_url = f"http://{host}:{mgmt_port}/pools/default/nodeServices"
        try:
            response = requests.get(node_services_url, auth=(username, password), timeout=3)
            response.raise_for_status()
            payload = response.json()
            nodes = payload.get("nodesExt", [])
            if not nodes:
                continue
            node = next((n for n in nodes if n.get("thisNode")), nodes[0])
            n1ql_port = node.get("services", {}).get("n1ql")
            if n1ql_port:
                return f"http://{host}:{n1ql_port}/query/service"
        except requests.exceptions.RequestException:
            continue

    fallback_endpoint = couchbase_config.get("query_endpoint")
    if fallback_endpoint:
        return fallback_endpoint

    return f"http://{host}:8093/query/service"
