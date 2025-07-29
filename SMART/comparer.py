import json

# Replace with the path to your JSON file
file_path = "./OldResults/test_debug_rag_exec2.json"

with open(file_path, "r") as f:
    data = json.load(f)

differences = []

for idx, example in enumerate(data):
    mql_original = example.get("MQL", "").strip()
    mql_debug = example.get("MQL_debug", "").strip()
    
    if mql_original != mql_debug:
        differences.append({
            "index": idx,
            "nlq": example.get("nlq", ""),
            "MQL": mql_original,
            "MQL_debug": mql_debug
        })

print(f"Total mismatches: {len(differences)}\n")

for diff in differences:
    print(f"Index: {diff['index']}")
    print(f"NLQ: {diff['nlq']}")
    print("Original MQL:")
    print(diff["MQL"])
    print("Debugged MQL:")
    print(diff["MQL_debug"])
    print("="*80)
