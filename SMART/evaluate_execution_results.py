import json
from tqdm import tqdm
import os
from utils.mongosh_exec import MongoShellExecutor

# Set paths
input_file = "./results/SMART/test_debug_rag_exec2.json"
output_file = "./results/SMART/test_debug_rag_exec2_results.json"

# Create output directory if it doesn't exist
os.makedirs(os.path.dirname(output_file), exist_ok=True)

# Initialize executor
executor = MongoShellExecutor()

# Load data
with open(input_file, "r") as f:
    test_data = json.load(f)

results_data = []

# Process each example
for idx, example in tqdm(enumerate(test_data), total=len(test_data)):
    db_id = example.get("db_id")
    nlq = example.get("nlq")
    query = example.get("MQL_debug_exec")

    # Execute the query
    result = executor.execute_query(db_name=db_id, query=query, get_str=True)

    results_data.append({
        "index": idx,
        "nlq": nlq,
        "db_id": db_id,
        "query": query,
        "execution_result": result
    })

# Save results
with open(output_file, "w") as f:
    json.dump(results_data, f, indent=4)

print(f"\n✅ Results saved to: {output_file}")
