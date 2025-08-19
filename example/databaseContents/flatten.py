#!/usr/bin/env python3
"""
Flatten testCopy.json into one NLQ per record.

INPUT:
  ./databaseContents/testCopy.json
  [
    {
      "record_id": 1861,
      "db_id": "school_bus",
      "nl_queries": ["...", "...", ...],
      "ref_sql": "...",
      "MQL": "..."
    },
    ...
  ]

OUTPUT:
  ./out/testCopy_flattened.json
  [
    {
      "record_id": "1861_1",
      "db_id": "school_bus",
      "nlq": "...",
      "ref_sql": "...",
      "MQL": "..."
    },
    ...
  ]
"""

import json
from pathlib import Path

# ---- Paths ----
INPUT_PATH  = Path("./testCopy.json")
OUTPUT_PATH = Path("./testCopy_flat.json")

def main():
    # Load input file
    with INPUT_PATH.open("r", encoding="utf-8") as f:
        records = json.load(f)

    flattened = []
    for rec in records:
        rec_id = rec.get("record_id")
        db_id = rec.get("db_id")
        ref_sql = rec.get("ref_sql", "")
        mql = rec.get("MQL", "")
        nl_queries = rec.get("nl_queries") or []

        # Expand each NLQ into its own object
        for i, nlq in enumerate(nl_queries, start=1):
            flattened.append({
                "record_id": f"{rec_id}_{i}",  # unique ID
                "db_id": db_id,
                "nlq": nlq,
                "SQL": ref_sql,
                "MQL": mql
            })

    # Ensure output directory exists
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)

    # Write flattened JSON
    with OUTPUT_PATH.open("w", encoding="utf-8") as f:
        json.dump(flattened, f, ensure_ascii=False, indent=2)

    print(f"Wrote {len(flattened)} records to {OUTPUT_PATH.resolve()}")

if __name__ == "__main__":
    main()
