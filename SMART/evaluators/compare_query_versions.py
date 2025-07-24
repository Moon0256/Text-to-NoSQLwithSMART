import json
import difflib
from tabulate import tabulate

def compare_queries(file_path: str):
    with open(file_path, 'r') as f:
        data = json.load(f)

    rows = []
    for ex in data:
        nlq = ex["nlq"]
        mql1 = ex.get("MQL", "").strip()
        mql2 = ex.get("MQL_debug", "").strip()
        mql3 = ex.get("MQL_debug_exec", "").strip()

        sim_1_3 = difflib.SequenceMatcher(None, mql1, mql3).ratio()
        sim_2_3 = difflib.SequenceMatcher(None, mql2, mql3).ratio()
        rows.append([
            nlq[:60] + "..." if len(nlq) > 60 else nlq,
            f"{sim_1_3:.2f}", f"{sim_2_3:.2f}"
        ])

    print(tabulate(rows, headers=["NLQ", "sim(MQL, exec)", "sim(debug, exec)"], tablefmt="grid"))

if __name__ == "__main__":
    compare_queries("../results/SMART/test_debug_rag_exec2.json")
