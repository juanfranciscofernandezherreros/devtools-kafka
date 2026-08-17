#!/usr/bin/env python3
"""Download Avro schemas from a Confluent Schema Registry.

Standard library only (urllib), so no extra pip install is needed.

Examples:
    # all subjects from the local Docker stack
    python download-schemas.py --registry-url http://localhost:8081

    # just the key/value schemas for one topic
    python download-schemas.py --registry-url http://localhost:8081 --topic test-topic

    # a specific subject, from a real environment with basic auth and a
    # self-signed / internal-CA certificate
    python download-schemas.py \\
        --registry-url https://rft-confluent-cito-rest-rft-int-bec.ingress.cph01.ocp.six-group.net \\
        --subject test-topic-value \\
        --user myuser --password mypass \\
        --insecure

Each schema is saved as <out-dir>/<subject>.avsc, pretty-printed.
"""

import argparse
import base64
import json
import ssl
import sys
import urllib.error
import urllib.request
from pathlib import Path

SCHEMA_REGISTRY_ACCEPT = "application/vnd.schemaregistry.v1+json, application/json"


def build_ssl_context(insecure: bool) -> ssl.SSLContext | None:
    if not insecure:
        return None
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    return ctx


def fetch_json(url: str, user: str | None, password: str | None, ssl_context):
    request = urllib.request.Request(url, headers={"Accept": SCHEMA_REGISTRY_ACCEPT})
    if user:
        token = base64.b64encode(f"{user}:{password or ''}".encode()).decode()
        request.add_header("Authorization", f"Basic {token}")

    try:
        with urllib.request.urlopen(request, context=ssl_context, timeout=15) as response:
            return json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", errors="replace")
        raise SystemExit(f"HTTP {e.code} calling {url}: {body}") from e
    except urllib.error.URLError as e:
        raise SystemExit(f"Could not reach {url}: {e.reason}") from e


def list_subjects(registry_url: str, user, password, ssl_context) -> list[str]:
    return fetch_json(f"{registry_url}/subjects", user, password, ssl_context)


def get_latest_schema(registry_url: str, subject: str, user, password, ssl_context) -> dict:
    return fetch_json(f"{registry_url}/subjects/{subject}/versions/latest", user, password, ssl_context)


def save_schema(subject: str, schema_response: dict, out_dir: Path) -> Path:
    out_dir.mkdir(parents=True, exist_ok=True)
    schema_str = schema_response["schema"]

    # The Avro schema itself is JSON, encoded as a string inside the
    # Schema Registry response. Parse + re-dump it for a readable file.
    try:
        schema_obj = json.loads(schema_str)
        pretty = json.dumps(schema_obj, indent=2, ensure_ascii=False)
    except json.JSONDecodeError:
        # Primitive schemas like "string" or "int" decode fine above too,
        # but fall back to the raw string just in case.
        pretty = schema_str

    file_path = out_dir / f"{subject}.avsc"
    file_path.write_text(pretty + "\n", encoding="utf-8")
    return file_path


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--registry-url", required=True, help="Schema Registry base URL, e.g. http://localhost:8081")
    parser.add_argument("--subject", action="append", default=[], help="Exact subject name; repeatable")
    parser.add_argument("--topic", action="append", default=[], help="Topic name; expands to <topic>-key and <topic>-value; repeatable")
    parser.add_argument("--out-dir", default="schemas", help="Output directory (default: ./schemas)")
    parser.add_argument("--insecure", action="store_true", help="Skip TLS certificate validation")
    parser.add_argument("--user", help="Basic auth username")
    parser.add_argument("--password", help="Basic auth password")
    args = parser.parse_args()

    registry_url = args.registry_url.rstrip("/")
    ssl_context = build_ssl_context(args.insecure)

    subjects = list(args.subject)
    for topic in args.topic:
        subjects += [f"{topic}-key", f"{topic}-value"]

    if not subjects:
        print(f"No --subject/--topic given, listing all subjects from {registry_url} ...")
        subjects = list_subjects(registry_url, args.user, args.password, ssl_context)

    if not subjects:
        print("No subjects found.")
        return

    out_dir = Path(args.out_dir)
    failures = []
    for subject in subjects:
        try:
            schema_response = get_latest_schema(registry_url, subject, args.user, args.password, ssl_context)
            path = save_schema(subject, schema_response, out_dir)
            print(f"OK   {subject} -> {path}")
        except SystemExit as e:
            print(f"FAIL {subject}: {e}")
            failures.append(subject)

    print(f"\n{len(subjects) - len(failures)}/{len(subjects)} schema(s) downloaded to {out_dir}/")
    if failures:
        sys.exit(1)


if __name__ == "__main__":
    main()
