#!/usr/bin/env python3
"""
Build formatted_results.json with fields (one row per NLQ):
  - count      : 1-based running index for each (db_id, nlq) pair
  - db_id      : database identifier (e.g., "school_bus")
  - nlq        : a single natural-language query from the nl_queries list
  - SQL        : the gold/reference SQL from testCopy.json (field: ref_sql)
  - SQL_pred   : the predicted SQL from output.json (field: sql), matched by db_id
  - MQL        : the gold/reference MongoDB query from testCopy.json (field: MQL)
  - MQL_pred   : the predicted MongoDB query from output.json (field: mongodb), matched by db_id

Inputs (paths are set below):
  - ./databaseContents/testCopy.json
  - ./out/output.json

Output:
  - ./out/formatted_results.json
"""

import json
from pathlib import Path

# ---- Direct paths (edit if your files live elsewhere) ----
# These Path objects point to where your input files live and where to write output.
TESTCOPY_PATH = Path("./databaseContents/testCopy.json")
OUTPUT_PATH   = Path("./out/output.json")
OUT_PATH      = Path("./out/formatted_results.json")


def load_json_array(p: Path):
    """
    Open a JSON file expected to contain a top-level array (list).
    - p: Path to the JSON file.
    Returns: the parsed Python list.
    Exits the program with a clear error if the file is missing or invalid.

    Why enforce "array"? Because the script assumes it will iterate over a list
    of records (objects). If the root is not a list, downstream code would break.
    """
    try:
        with p.open("r", encoding="utf-8") as f:
            data = json.load(f)

        if not isinstance(data, list):
            raise ValueError(f"{p} is not a JSON array.")
        return data

    except FileNotFoundError:
        raise SystemExit(f"ERROR: File not found: {p}")

    except json.JSONDecodeError as e:
        raise SystemExit(f"ERROR: Invalid JSON in {p}: {e}")


def build_dbid_pred_maps(output_rows):
    """
    Build two dictionaries keyed by db_id using rows from output.json:
      - dbid_to_mql_pred : db_id -> mongodb (predicted MQL string)
      - dbid_to_sql_pred : db_id -> sql     (predicted SQL string, as echoed by server)

    Behavior:
      - Trims db_id strings.
      - Keeps the FIRST non-empty value per db_id encountered (stable and simple).
        If your output.json has multiple rows per db_id and you prefer
        "last one wins", change the `if dbid not in mapping` checks below.

    Returns:
      (dbid_to_mql_pred, dbid_to_sql_pred)
    """
    dbid_to_mql_pred = {}
    dbid_to_sql_pred = {}

    for row in output_rows:
        dbid = str(row.get("db_id", "")).strip()
        if not dbid:
            continue

        # Predicted Mongo pipeline from the translator response
        mongodb = row.get("mongodb")
        if mongodb and dbid not in dbid_to_mql_pred:
            dbid_to_mql_pred[dbid] = mongodb

        # Predicted SQL echoed back by the server (or whatever you saved)
        sql_pred = row.get("sql")
        if sql_pred and dbid not in dbid_to_sql_pred:
            dbid_to_sql_pred[dbid] = sql_pred

    return dbid_to_mql_pred, dbid_to_sql_pred


def main():
    # Load both input arrays (lists of records).
    test_rows = load_json_array(TESTCOPY_PATH)
    output_rows = load_json_array(OUTPUT_PATH)

    # Build quick lookups by db_id for predicted MQL and predicted SQL.
    dbid_to_mql_pred, dbid_to_sql_pred = build_dbid_pred_maps(output_rows)

    combined = []  # This will accumulate one object per NLQ.
    count = 1      # 1-based counter for rows in the final output.

    # Iterate over each record in testCopy.json.
    # Expected structure (fields used here):
    #   - "db_id"     : string
    #   - "nl_queries": list of strings (paraphrases)
    #   - "ref_sql"   : string (gold SQL)      -> becomes "SQL"
    #   - "MQL"       : string (gold MongoDB)  -> becomes "MQL"
    for rec in test_rows:
        db_id = str(rec.get("db_id", "")).strip()
        nl_queries = rec.get("nl_queries") or []

        # Gold/reference SQL and MQL from testCopy.json
        sql_gold = rec.get("ref_sql", "")  # may be missing; default to ""
        mql_gold = rec.get("MQL", "")

        # Predicted SQL/MQL looked up by db_id from output.json
        sql_pred = dbid_to_sql_pred.get(db_id, "")
        mql_pred = dbid_to_mql_pred.get(db_id, "")

        # Create one output row PER NLQ so your evaluator can align
        # specific NLQs with the same gold/predicted SQL/MQL.
        for nlq in nl_queries:
            combined.append({
                "count": count,      # Running index
                "db_id": db_id,      # e.g., "school_bus"
                "nlq": nlq,          # natural-language query string

                # NEW fields requested:
                "SQL": sql_gold,     # gold/reference SQL from testCopy.json.ref_sql
                "SQL_pred": sql_pred,  # predicted SQL from output.json.sql

                # Existing fields:
                "MQL": mql_gold,     # gold/reference MQL from testCopy.json.MQL
                "MQL_pred": mql_pred # predicted MQL from output.json.mongodb
            })
            count += 1

    # Write output
    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with OUT_PATH.open("w", encoding="utf-8") as f:
        json.dump(combined, f, ensure_ascii=False, indent=2)

    print(f"Wrote {len(combined)} rows to {OUT_PATH.resolve()}")


if __name__ == "__main__":
    main()
