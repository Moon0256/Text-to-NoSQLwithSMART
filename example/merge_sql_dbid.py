#!/usr/bin/env python3
"""
Merge a text file of SQLs with a JSON file of DB assignments into one file,
matching strictly by ORDER (1st SQL ↔ 1st JSON item, etc.), ignoring any record_id
present in the JSON DB file.

Output records look like:
  {"db_id": "<database name>", "question": "<SQL string>"}

Use --out_format jsonl (default) or json.

Run in terminal:
python merge_sql_dbid.py \
  --sql_txt databaseContents/DAILresults.txt \
  --db_json databaseContents/dbidQuest.json \
  --out out/merged.jsonl \
  --out_format jsonl

"""

import argparse
import json
from pathlib import Path
from typing import Any, Dict, List

def read_sqls(sql_path: Path) -> List[str]:
    # (unchanged) read non-empty lines
    with sql_path.open("r", encoding="utf-8") as f:
        return [line.strip() for line in f if line.strip()]

def read_db_list(db_path: Path) -> List[str]:
    """
    ### CHANGED: We now ONLY read the DB file as an ORDERED LIST,
    ### ignoring any record_id inside objects.

    Accepted shapes:
      1) list of strings: ["cre_Doc_Template_Mgt", "tpch", ...]
      2) list of objects (any order/keys): we will extract a db id from
         one of the keys: "db_id", "db", "database". If multiple are present,
         priority is db_id > db > database. record_id is ignored.
    """
    with db_path.open("r", encoding="utf-8") as f:
        data = json.load(f)

    if isinstance(data, list):
        # list of strings?
        if all(isinstance(x, str) for x in data):
            return data

        # list of objects → extract db_id/db/database by order
        if all(isinstance(x, dict) for x in data):
            out: List[str] = []
            for obj in data:
                # ### CHANGED: extract by priority, ignore record_id entirely
                db = obj.get("db_id")
                if db is None:
                    db = obj.get("db")
                if db is None:
                    db = obj.get("database")
                if db is None:
                    raise SystemExit(
                        "DB JSON list contains an object without any of: 'db_id', 'db', 'database'."
                    )
                out.append(str(db))
            return out

    # Give a helpful error if shape is not supported
    raise SystemExit(
        "Unsupported db_json shape. Provide a JSON array that is either:\n"
        "  - a list of DB strings, e.g. [\"cre_Doc_Template_Mgt\", \"tpch\", ...]\n"
        "  - a list of objects where each object has one of: 'db_id', 'db', or 'database'\n"
        "Record IDs will be IGNORED; items are matched strictly by LIST ORDER."
    )

def write_out(records: List[Dict[str, Any]], out_path: Path, fmt: str) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    if fmt == "jsonl":
        with out_path.open("w", encoding="utf-8") as f:
            for rec in records:
                f.write(json.dumps(rec, ensure_ascii=False) + "\n")
    else:
        with out_path.open("w", encoding="utf-8") as f:
            json.dump(records, f, ensure_ascii=False, indent=2)

def main():
    ap = argparse.ArgumentParser(
        description="Merge SQL TXT with DB JSON (match by order only) into JSON/JSONL of {'db_id','question'}."
    )
    ap.add_argument("--sql_txt", required=True, help="Path to TXT with one SQL per line.")
    ap.add_argument("--db_json", required=True, help="Path to JSON with DB entries (list).")
    ap.add_argument("--out", required=True, help="Output path (.jsonl or .json).")
    ap.add_argument("--out_format", choices=["jsonl", "json"], default="jsonl",
                    help="Output format (default: jsonl).")
    args = ap.parse_args()

    sqls = read_sqls(Path(args.sql_txt))
    dbs  = read_db_list(Path(args.db_json))

    # ### CHANGED: strict by-order pairing; lengths must match
    if len(dbs) < len(sqls):
        raise SystemExit(
            f"DB list has {len(dbs)} entries but there are {len(sqls)} SQLs. "
            "Add more DB entries or remove extra SQL lines."
        )
    if len(dbs) > len(sqls):
        # It's okay to be strict; you can relax this if you like.
        raise SystemExit(
            f"DB list has {len(dbs)} entries but there are {len(sqls)} SQLs. "
            "Remove extra DB entries or add more SQL lines."
        )

    # Build output: [{"db_id": ..., "question": ...}, ...]
    # ### CHANGED: output keys renamed to db_id + question
    merged = [{"db_id": dbs[i], "sql": sqls[i]} for i in range(len(sqls))]

    write_out(merged, Path(args.out), args.out_format)
    print(f"[OK] Wrote {len(merged)} records to {args.out} ({args.out_format}).")

if __name__ == "__main__":
    main()
