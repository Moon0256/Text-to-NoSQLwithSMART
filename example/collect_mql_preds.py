#!/usr/bin/env python3
"""
collect_mql_preds.py  —  Send SQLs to the Java SQL→Mongo translator server and save results.

INPUT
  • JSONL: one JSON object per line
  • JSON : an array of objects
Each object must contain:
  - "db_id": <Mongo database name>
  - one of "question" | "sql" | "ref_sql"  (we normalize to "sql")

OUTPUT
  • JSON array with objects of the form:
      {
        "db_id": "<db name>",
        "sql":   "<original SQL>",
        "mongodb": "<MongoDB query string, or 'ERROR: ...'>"
      }

SERVER CONTRACT (your Java server)
  - Endpoint: /translate
  - Method:   GET
  - Params:   db=<db_id>, sql=<SQL string URL-encoded>
    e.g. http://localhost:8082/translate?db=tpch&sql=SELECT%20*%20FROM%20nation

Run example:
  python collect_mql_preds.py \
  --in out/merged.jsonl \
  --out out/output.json \
  --url http://localhost:8082/translate \
  --method get \
  --debug \
  --probe
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests # Standard library for HTTP requests: This is the idiomatic way to do GET with query parameters in Python requests. For long queries, use POST with a JSON body (if/when your server supports it).

# ---------------- Defaults (single source of truth) ----------------
DEFAULT_URL: str = "http://localhost:8082/translate"
DEFAULT_METHOD: str = "get"        # only GET is actually supported by your server
DEFAULT_TIMEOUT: float = 90.0      # generous to avoid local delays
DEFAULT_RETRIES: int = 2           # simple retry loop (on our side)


# ===================== Input loader =====================
def load_records(path: Path) -> List[Dict[str, str]]:
    """
    Read a JSONL file OR a JSON array file and normalize to: (so trim and stuff done here)
      {"db_id": "<db>", "sql": "<sql>"}

    - If the input object has "question", we prefer that; otherwise "sql"; otherwise "ref_sql".
    - We raise a friendly error if "db_id" or the SQL field is missing.
    """
    text = path.read_text(encoding="utf-8").strip()
    raw: List[Any] = []

    # Detect JSON array vs JSON Lines by first non-space char
    if text.startswith("["):
        data = json.loads(text)
        if not isinstance(data, list):
            raise SystemExit("Top-level JSON must be an array.")
        raw = data
    else:
        # JSONL: one JSON object per line
        for ln, line in enumerate(text.splitlines(), start=1):
            line = line.strip()
            if not line:
                continue
            try:
                raw.append(json.loads(line))
            except json.JSONDecodeError as e:
                raise SystemExit(f"Invalid JSON on line {ln}: {e}")

    out: List[Dict[str, str]] = []
    for i, obj in enumerate(raw, start=1):
        if not isinstance(obj, dict):
            raise SystemExit(f"Record #{i} is not an object (got {type(obj)}).")
        db = obj.get("db_id")
        # Prefer "question" when present; else "sql"; else "ref_sql"
        sql = obj.get("question") or obj.get("sql") or obj.get("ref_sql")
        if not db or not sql:
            raise SystemExit(f"Record #{i} missing 'db_id' or 'question/sql'. Object={obj}")
        out.append({"db_id": str(db).strip(), "sql": str(sql)})
    return out


# ===================== HTTP helpers =====================
def _prepare_url_for_get(url: str, params: Dict[str, str]) -> str:
    """
    Build the exact URL that requests will hit for GET (with proper URL encoding),
    so you can print it in --debug and compare with what the Java server expects.
    """
    req = requests.Request("GET", url, params=params)
    prepped = req.prepare()
    return prepped.url  # fully encoded


def _call_get(url: str, db: str, sql: str, timeout: float, debug: bool) -> Dict[str, Any]:
    """
    Call your Java server using GET with ?db=<db>&sql=<url-encoded>.
    Returns JSON body as dict (or raises with status+body on failure).
    """
    params = {"db": db, "sql": sql}
    if debug:
        print("[DEBUG] GET URL ->", _prepare_url_for_get(url, params))

    r = requests.get(url, params=params, timeout=timeout)
    # Your server always returns JSON; if not, show raw text for debugging.
    try:
        data = r.json()
    except Exception:
        # If response is not valid JSON, surface status + text to help debug
        raise SystemExit(f"Server returned non-JSON (status={r.status_code}):\n{r.text}")

    if r.status_code >= 400:
        # Your server includes {"error": "..."} on failures — preserve that.
        err = data.get("error") or f"HTTP {r.status_code}"
        raise requests.HTTPError(f"{err} | body={data}")
    return data


def _call_post(url: str, db: str, sql: str, timeout: float, debug: bool) -> Dict[str, Any]:
    """
    POST version. NOTE: your Java handler currently ONLY supports GET.
    Use this only if you add a POST handler on the server.
    """
    payload = {"db": db, "sql": sql}
    if debug:
        print("[DEBUG] POST", url, "JSON payload ->", json.dumps(payload)[:300], "...")
    r = requests.post(url, json=payload, timeout=timeout)
    try:
        data = r.json()
    except Exception:
        raise SystemExit(f"Server returned non-JSON (status={r.status_code}):\n{r.text}")

    if r.status_code >= 400:
        err = data.get("error") or f"HTTP {r.status_code}"
        raise requests.HTTPError(f"{err} | body={data}")
    return data


def fetch_mql(url: str, method: str, db: str, sql: str,
              timeout: float, retries: int, debug: bool) -> str:
    """
    Try to fetch the translated Mongo string for one (db, sql).
    - Uses GET (your server’s contract) unless method='post' explicitly set AND server supports it.
    - On HTTP errors, we retry a few times, then return "ERROR: <last error>".
    """
    last_err: Optional[BaseException] = None
    for attempt in range(retries + 1):
        try:
            if method == "get":
                data = _call_get(url, db, sql, timeout, debug)
            else:
                data = _call_post(url, db, sql, timeout, debug)
            mongo = data.get("mongo", "")
            # normalize to string; your server returns a string
            return mongo if isinstance(mongo, str) else str(mongo)
        except Exception as e:
            last_err = e
            if debug:
                print(f"[DEBUG] Attempt {attempt+1}/{retries+1} failed: {e}")
    return f"ERROR: {last_err}"


# ===================== Batch driver =====================
def run_batch(records: List[Dict[str, str]], server_url: str, method: str,
              timeout: float, retries: int, debug: bool) -> List[Dict[str, Any]]:
    """
    Loop through normalized {"db_id","sql"} records, call the server,
    and build the output array with {"db_id","sql","mongodb"}.
    """
    out: List[Dict[str, Any]] = []
    # Loop through each record(which is dict of db_id and sql), fetch MQL, and build output
    for idx, rec in enumerate(records, start=1):
        db = rec["db_id"]
        sql = rec["sql"]
        if debug:
            print(f"\n[DEBUG] ---- Record #{idx} ----")
            print("[DEBUG] db_id =", db)
            print("[DEBUG] sql   =", sql[:500], "..." if len(sql) > 500 else "")
        mql = fetch_mql(server_url, method, db, sql, timeout, retries, debug)
        out.append({"db_id": db, "sql": sql, "mongodb": mql})
    return out


# ===================== (Optional) quick server check =====================
def verify_server_reachable(url: str, debug: bool) -> None:
    """
    Quick probe to catch obvious mistakes (wrong host/port/path).
    We hit the endpoint WITHOUT params to see if server returns a helpful 400/405.
    """
    try:
        r = requests.get(url, timeout=5)
        if debug:
            print("[DEBUG] Server probe:", url, "->", r.status_code, r.text[:200])
    except Exception as e:
        raise SystemExit(f"Cannot reach server at {url} : {e}")


# ===================== CLI =====================
def main() -> None:
    ap = argparse.ArgumentParser(description="Collect MQL translations from Java server.")
    ap.add_argument("--in", dest="in_path", required=True,
                    help="Path to JSONL (preferred) or JSON array with {'db_id','question'|'sql'|'ref_sql'}.")
    ap.add_argument("--out", dest="out_path", default="out/output.json",
                    help="Path to write output JSON (default: out/output.json).")
    ap.add_argument("--url", default=DEFAULT_URL,
                    help=f"Translator endpoint (default: {DEFAULT_URL})")
    ap.add_argument("--method", default=DEFAULT_METHOD, choices=["get", "post"],
                    help="HTTP method to use (default: get). Your server supports GET only.")
    ap.add_argument("--timeout", type=float, default=DEFAULT_TIMEOUT,
                    help=f"HTTP timeout seconds (default: {DEFAULT_TIMEOUT})")
    ap.add_argument("--retries", type=int, default=DEFAULT_RETRIES,
                    help=f"Retry attempts on failure (default: {DEFAULT_RETRIES})")
    ap.add_argument("--debug", action="store_true",
                    help="Print full prepared request (URL/payload) and verbose errors.")
    ap.add_argument("--probe", action="store_true",
                    help="Ping the server URL before sending any records.")
    args = ap.parse_args()

    in_path = Path(args.in_path)
    out_path = Path(args.out_path)

    # Load and normalize input records
    records = load_records(in_path)
    # So, records is now a list of {"db_id", "sql"} dicts
    if not records:
        raise SystemExit("No records found in input.")

    # Optional: quick probe to catch wrong URL/port/path early
    if args.probe:
        verify_server_reachable(args.url, debug=args.debug)

    # Batch translate
    preds = run_batch(records, args.url, args.method.lower(),
                      args.timeout, args.retries, args.debug)

    # Write output JSON
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", encoding="utf-8") as f:
        json.dump(preds, f, ensure_ascii=False, indent=2)
    print(f"[OK] Wrote {len(preds)} rows to {out_path}")


if __name__ == "__main__":
    main()
