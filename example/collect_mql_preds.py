#!/usr/bin/env python3
"""
Send SQLs to the Java SQL→Mongo translator server and save results.

INPUT (expected):
  • JSONL: one JSON object per line, each like {"db_id": "...", "question": "..."}
  • JSON : an array of such objects

OUTPUT:
  • JSON array of:
      {
        "db_id": "<db name>",
        "sql": "<original SQL>",
        "mongodb": "<MongoDB query string or 'ERROR: ...'>"
      }

Server default (Ramon):
  GET http://localhost:8082/translate?db=<db>&sql=<url-encoded>

Run in terminal:
python collect_mql_preds.py \
  --in out/merged.jsonl \
  --out out/output.json \
  --url http://localhost:8082/translate \
  --method get

"""

import argparse
import json
from pathlib import Path
from typing import Any, Dict, List

import requests

DEFAULT_URL = "http://localhost:8082/translate"
DEFAULT_METHOD = "get"               # "get" or "post"
DEFAULT_TIMEOUT = 30.0
DEFAULT_RETRIES = 2


# ---------- input loader ----------

# CHANGED: load JSONL or JSON array with {"db_id","question"}; normalize to {"db_id","sql"}
def load_records(path: Path) -> List[Dict[str, str]]:
    text = path.read_text(encoding="utf-8").strip()
    records: List[Dict[str, Any]] = []
    if text.startswith("["):
        data = json.loads(text)
        if not isinstance(data, list):
            raise SystemExit("Top-level JSON must be an array.")
        records = data
    else:
        for line in text.splitlines():
            if not line.strip():
                continue
            records.append(json.loads(line))

    out: List[Dict[str, str]] = []
    for i, obj in enumerate(records, start=1):
        if not isinstance(obj, dict):
            raise SystemExit(f"Record #{i} is not an object.")
        db = obj.get("db_id")
        # Accept "question" (preferred), or fallbacks "sql"/"ref_sql"
        sql = obj.get("question") or obj.get("sql") or obj.get("ref_sql")
        if not db or not sql:
            raise SystemExit(f"Record #{i} missing 'db_id' or 'question/sql'.")
        out.append({"db_id": str(db), "sql": str(sql)})
    return out


# ---------- network calls ----------

DEFAULT_URL = "http://localhost:8082/translate"
DEFAULT_METHOD = "get"
DEFAULT_TIMEOUT = 90.0            # CHANGE: was 30.0
DEFAULT_RETRIES = 2

def call_server_get(url: str, db: str, sql: str, timeout: float) -> Dict[str, Any]:
    r = requests.get(url, params={"db": db, "sql": sql}, timeout=timeout)
    # CHANGE: try to read JSON even on non-2xx to surface server error message
    try:
        data = r.json()
    except Exception:
        r.raise_for_status()
        return {}
    if r.status_code >= 400:
        # Promote server-side "error" into an HTTPError so fetch_mql captures it
        err = data.get("error") or f"HTTP {r.status_code}"
        raise requests.HTTPError(err)
    return data

def call_server_post(url: str, db: str, sql: str, timeout: float) -> Dict[str, Any]:
    r = requests.post(url, json={"db": db, "sql": sql}, timeout=timeout)
    try:
        data = r.json()
    except Exception:
        r.raise_for_status()
        return {}
    if r.status_code >= 400:
        err = data.get("error") or f"HTTP {r.status_code}"
        raise requests.HTTPError(err)
    return data

# CHANGED: single fetch that uses GET/POST and retries; returns the "mongo" string (or ERROR)
def fetch_mql(url: str, method: str, db: str, sql: str, timeout: float, retries: int) -> str:
    last_err = None
    for _ in range(retries + 1):
        try:
            data = (call_server_get if method == "get" else call_server_post)(
                url, db, sql, timeout
            )
            mongo = data.get("mongo", "")
            if not isinstance(mongo, str):
                mongo = str(mongo)
            return mongo
        except Exception as e:
            last_err = e
    return f"ERROR: {last_err}"


# ---------- batch driver ----------

# CHANGED: run over per-record {"db_id","sql"}; include db_id & sql in output
def run_batch(records: List[Dict[str, str]], server_url: str, method: str,
              timeout: float, retries: int) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for rec in records:
        db = rec["db_id"]
        sql = rec["sql"]
        mql = fetch_mql(server_url, method, db, sql, timeout, retries)
        out.append({
            "db_id": db,
            "sql": sql,
            "mongodb": mql,
        })
    return out


# ---------- CLI ----------

def main() -> None:
    ap = argparse.ArgumentParser(description="Collect MQL translations from Java server.")
    ap.add_argument("--in", dest="in_path", required=True,
                    help="Path to JSONL (preferred) or JSON array with {'db_id','question'}.")
    ap.add_argument("--out", dest="out_path", default="out/output.json",
                    help="Path to write output JSON (default: out/output.json).")
    ap.add_argument("--url", default=DEFAULT_URL,
                    help=f"Translator endpoint (default: {DEFAULT_URL})")
    ap.add_argument("--method", default=DEFAULT_METHOD, choices=["get", "post"],
                    help="HTTP method to use (default: get).")
    ap.add_argument("--timeout", type=float, default=DEFAULT_TIMEOUT,
                    help=f"HTTP timeout seconds (default: {DEFAULT_TIMEOUT})")
    ap.add_argument("--retries", type=int, default=DEFAULT_RETRIES,
                    help=f"Retry attempts on failure (default: {DEFAULT_RETRIES})")
    args = ap.parse_args()

    in_path = Path(args.in_path)
    out_path = Path(args.out_path)

    # CHANGED: single loader; no TXT mode, no --db flag, no --format needed
    records = load_records(in_path)
    if not records:
        raise SystemExit("No records found in input.")

    preds = run_batch(records, args.url, args.method, args.timeout, args.retries)

    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", encoding="utf-8") as f:
        json.dump(preds, f, ensure_ascii=False, indent=2)
    print(f"[OK] Wrote {len(preds)} rows to {out_path}")


if __name__ == "__main__":
    main()
