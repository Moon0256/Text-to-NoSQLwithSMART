#!/usr/bin/env python3
"""
Build formatted_results.json with fields (one row per NLQ):
  - count     : 1-based running index for each (db_id, nlq) pair
  - db_id     : database identifier (e.g., "school_bus")
  - nlq       : a single natural-language query from the nl_queries list
  - MQL       : the gold/reference MongoDB query from testCopy.json
  - MQL_pred  : the predicted MongoDB query from output.json (matched by db_id)

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
        # Open file with UTF-8 to support any non-ASCII characters.
        with p.open("r", encoding="utf-8") as f:
            data = json.load(f)  # Parse JSON into Python structures.

        # Ensure the top-level structure is a list/array.
        if not isinstance(data, list):
            raise ValueError(f"{p} is not a JSON array.")
        return data

    # If the path is wrong or file missing, exit with a descriptive message.
    except FileNotFoundError:
        raise SystemExit(f"ERROR: File not found: {p}")

    # If JSON is malformed (trailing commas, bad quotes, etc.), exit cleanly.
    except json.JSONDecodeError as e:
        raise SystemExit(f"ERROR: Invalid JSON in {p}: {e}")


def build_dbid_to_mongodb_map(output_rows):
    """
    Build a dictionary mapping db_id -> mongodb (predicted MQL).

    Parameters:
      - output_rows: list of objects from output.json, each expected to have:
            {
              "db_id": "...",
              "mongodb": "..."  # predicted MongoDB pipeline string
              ...
            }

    Behavior:
      - Trims db_id strings.
      - Keeps the first non-empty 'mongodb' per db_id encountered.
        (If there are multiple records per db_id, we intentionally keep the first.
         You can change this behavior to "last one wins" or "collect all" as needed.)
    Returns:
      - dict like {"school_bus": "db.school_bus.aggregate([...])", ...}
    """
    mapping = {}
    for row in output_rows:
        # Get db_id; coerce to str (in case None) and strip whitespace.
        dbid = str(row.get("db_id", "")).strip()
        # Extract the predicted MongoDB query pipeline string.
        mongodb = row.get("mongodb")

        # Only store if we have both a dbid and a non-empty mongodb
        # and we haven't already stored a value for this dbid.
        if dbid and mongodb and dbid not in mapping:
            mapping[dbid] = mongodb

    return mapping


def main():
    # Load both input arrays (lists of records).
    test_rows = load_json_array(TESTCOPY_PATH)
    output_rows = load_json_array(OUTPUT_PATH)

    # Build a quick lookup: db_id -> predicted mongodb query string.
    dbid_to_mql_pred = build_dbid_to_mongodb_map(output_rows)

    combined = []  # This will accumulate one object per NLQ.
    count = 1      # 1-based counter for rows in the final output.

    # Iterate over each record in testCopy.json.
    # Each record typically looks like:
    # {
    #   "record_id": 1861,
    #   "db_id": "school_bus",
    #   "nl_queries": [... 5 paraphrases ...],
    #   "ref_sql": "...",
    #   "MQL": "... gold Mongo query ..."
    # }
    for rec in test_rows:
        # Normalize db_id to a trimmed string.
        db_id = str(rec.get("db_id", "")).strip()

        # Pull the list of NLQs; default to empty list if missing/None.
        nl_queries = rec.get("nl_queries") or []

        # Gold/reference MongoDB query string (can be empty if missing).
        mql_gold = rec.get("MQL", "")

        # Predicted MQL from the output map; empty string if no match.
        mql_pred = dbid_to_mql_pred.get(db_id, "")

        # Create one output row **per** NLQ so your evaluator can align
        # specific NLQs with the same gold/predicted MQL.
        for nlq in nl_queries:
            combined.append({
                "count": count,     # Running index
                "db_id": db_id,     # e.g., "school_bus"
                "nlq": nlq,         # the single NLQ string
                "MQL": mql_gold,    # gold/reference MongoDB query from testCopy.json
                "MQL_pred": mql_pred  # predicted MongoDB query from output.json
            })
            count += 1

    # Ensure the output directory exists (parent dirs of OUT_PATH), then write JSON.
    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)

    # Write the combined results as pretty-printed JSON (indent=2).
    with OUT_PATH.open("w", encoding="utf-8") as f:
        json.dump(combined, f, ensure_ascii=False, indent=2)

    # Print a friendly summary for CLI users.
    print(f"Wrote {len(combined)} rows to {OUT_PATH.resolve()}")


# Standard Python entry point guard.
# Ensures main() only runs when this file is executed as a script, not when imported.
if __name__ == "__main__":
    main()
