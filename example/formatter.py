#!/usr/bin/env python3
"""
Strict 1:1 join:
- Zips testCopy_flat.json (gold) with output.json (preds) by position
- Writes formatted_results.json with: count, db_id, nlq, SQL, SQL_pred, MQL, MQL_pred

Inputs:
  - ./out/testCopy_flat.json   # each row: {db_id, nlq, SQL, MQL, ...}
  - ./out/output.json          # each row: {sql, mongodb, ...} (same order & length)

Output:
  - ./out/formatted_results.json
"""

import json
from pathlib import Path

TESTCOPY_FLAT_PATH = Path("./databaseContents/testCopy_flat.json")
OUTPUT_PATH        = Path("./out/output.json")
OUT_PATH           = Path("./out/formatted_results.json")


def load_json_array(p: Path):
    with p.open("r", encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, list):
        raise SystemExit(f"ERROR: {p} does not contain a top-level JSON array.")
    return data


def main():
    gold_rows = load_json_array(TESTCOPY_FLAT_PATH)   # has db_id, nlq, SQL, MQL
    pred_rows = load_json_array(OUTPUT_PATH)          # has sql (-> SQL_pred), mongodb (-> MQL_pred)

    if len(gold_rows) != len(pred_rows):
        raise SystemExit(
            f"ERROR: Requires same length for both test file and prediction (output)."
            f"Test={len(gold_rows)} vs Preds={len(pred_rows)}."
        )

    combined = []
    for idx, (g, p) in enumerate(zip(gold_rows, pred_rows), start=1):
        db_id    = str(g.get("db_id", "")).strip()
        nlq      = g.get("nlq", "")
        sql_gold = g.get("SQL", "")
        mql_gold = g.get("MQL", "")

        # predictions from output.json
        sql_pred = p.get("sql", "") or ""
        mql_pred = p.get("mongodb", "") or ""

        combined.append({
            "count": idx,
            "db_id": db_id,
            "nlq": nlq,
            "SQL": sql_gold,
            "SQL_pred": sql_pred,
            "MQL": mql_gold,
            "MQL_pred": mql_pred
        })

    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with OUT_PATH.open("w", encoding="utf-8") as f:
        json.dump(combined, f, ensure_ascii=False, indent=2)

    print(f"Wrote {len(combined)} rows to {OUT_PATH.resolve()}")


if __name__ == "__main__":
    main()
