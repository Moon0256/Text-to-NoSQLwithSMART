#!/usr/bin/env python3
"""
Send SQLs to the Java SQL→Mongo translator server and save MQL predictions.

Reads either:
  • TXT: one SQL per line (blank lines ignored)
  • JSONL: one JSON object per line; must have 'sql' or 'ref_sql' field

Writes:
  • JSON array where each item is:
      {
        "record_id": <starting at 1>,
        "MQL_pred": "<MongoDB query string>"
      }
"""

import argparse
import json
from pathlib import Path
from typing import Any, Dict, List

import requests

DEFAULT_URL = "http://localhost:8080/translate"


# ---------- input readers ----------

def read_txt(path: Path) -> List[str]:
    with path.open("r", encoding="utf-8") as f:
        return [line.strip() for line in f if line.strip()]


def read_jsonl(path: Path) -> List[str]:
    sqls: List[str] = []
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            obj = json.loads(line)
            sql = obj.get("sql") or obj.get("ref_sql")
            if sql:
                sqls.append(sql)
    return sqls


# ---------- network call ----------

def post_sql(url: str, sql: str, timeout: float = 30.0) -> str:
    """
    Send one SQL to the Java server.
    Expects server to return JSON: {"mongo": "..."}.
    Returns the mongo string (or empty if missing).
    """
    r = requests.post(url, json={"sql": sql}, timeout=timeout)
    r.raise_for_status()
    data = r.json()
    return data.get("mongo", "")


# ---------- batch driver ----------

def run_batch(sqls: List[str], server_url: str) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for i, sql in enumerate(sqls, start=1):
        mql = ""
        try:
            mql = post_sql(server_url, sql)
        except Exception as e:
            mql = f"ERROR: {e}"
        out.append({
            "record_id": i,
            "MQL_pred": mql
        })
    return out


# ---------- CLI ----------

def main() -> None:
    ap = argparse.ArgumentParser(description="Collect MQL predictions from Java translator server.")
    ap.add_argument("--in", dest="in_path", required=True,
                    help="Input file: .txt (one SQL per line) or .jsonl (objects with 'sql' or 'ref_sql').")
    ap.add_argument("--out", dest="out_path", required=True,
                    help="Path to write predictions JSON.")
    ap.add_argument("--url", default=DEFAULT_URL,
                    help=f"Translator endpoint (default: {DEFAULT_URL})")
    ap.add_argument("--format", dest="fmt", choices=["txt", "jsonl"],
                    help="Force parser (auto-detected by extension if omitted).")
    args = ap.parse_args()

    in_path = Path(args.in_path)
    out_path = Path(args.out_path)

    # Detect input format
    fmt = args.fmt
    if fmt is None:
        fmt = "jsonl" if in_path.suffix.lower() == ".jsonl" else "txt"

    # Load inputs
    if fmt == "txt":
        sqls = read_txt(in_path)
    else:
        sqls = read_jsonl(in_path)

    if not sqls:
        raise SystemExit("No SQLs found in input file.")

    # Call server
    preds = run_batch(sqls, args.url)

    # Write predictions
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", encoding="utf-8") as f:
        json.dump(preds, f, ensure_ascii=False, indent=2)
    print(f"[OK] Wrote predictions to: {out_path}")


if __name__ == "__main__":
    main()
