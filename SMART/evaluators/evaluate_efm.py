#This script computes Execution Field Match (EFM) by comparing the fields retrieved by the executed MongoDB query with the expected target fields.

import json
import re
from typing import List, Set
from statistics import mean
from tabulate import tabulate

def extract_fields_from_mql(mql: str) -> Set[str]:
    """
    Extract fields from projection in MongoDB queries.
    Works for both `.find({filter}, {projection})` and `$project` stages.
    """
    fields = set()

    # Match .find({}, {field1: 1, field2: 1})
    find_projection = re.search(r'\.find\([^)]*,\s*{([^}]*)}', mql)
    if find_projection:
        proj_body = find_projection.group(1)
        for field in re.findall(r"'([^']+)'|\"([^\"]+)\"", proj_body):
            fields.add(field[0] or field[1])
        return fields

    # Match $project stages: { $project: { field1: 1, field2: 1 } }
    project_stages = re.findall(r'\$project\s*:\s*{([^}]*)}', mql)
    for stage in project_stages:
        for field in re.findall(r"'([^']+)'|\"([^\"]+)\"", stage):
            fields.add(field[0] or field[1])

    return fields

def compute_efm_score(pred_fields: Set[str], target_fields: Set[str]) -> float:
    if not target_fields:
        return 1.0 if not pred_fields else 0.0
    correct = len(pred_fields & target_fields)
    return correct / len(target_fields)

def evaluate(file_path: str):
    with open(file_path, 'r') as f:
        data = json.load(f)

    rows = []
    scores = []
    for ex in data:
        nlq = ex["nlq"]
        mql = ex["MQL_debug_exec"]
        target = set(ex["target_fields"].replace(" ", "").split(","))
        pred = extract_fields_from_mql(mql)

        efm = compute_efm_score(pred, target)
        scores.append(efm)
        rows.append([nlq, ", ".join(pred), ", ".join(target), f"{efm:.2f}"])

    print(tabulate(rows, headers=["NLQ", "Predicted Fields", "Target Fields", "EFM"], tablefmt="grid"))
    print(f"\n✅ Average EFM Score: {mean(scores):.3f}")
    print(f"✅ Exact Match (EFM = 1.0): {sum([s==1.0 for s in scores])}/{len(scores)}")

if __name__ == "__main__":
    evaluate("./results/SMART/test_debug_rag_exec2.json")
