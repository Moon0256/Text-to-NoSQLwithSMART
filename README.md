# Text-to-NoSQL: Natural Language Interface for MongoDB

This project enables querying **MongoDB** databases using **Natural language questions** by translating them into MongoDB queries (MQL)

> **Based on the paper and code from:**  
> [Bridging the Gap: Enabling Natural Language Queries for NoSQL Databases through Text-to-NoSQL Translation](https://arxiv.org/pdf/2502.11201)

---
# Text-to-NoSQL Pipeline

## Overview

This project translates **Natural Language Queries (NLQ)** → **SQL/Mongo**, collects predicted MongoDB queries, formats the results, and evaluates them with a metrics suite.

There are two ways to run it:

- **All-in-one** with `run_pipeline.sh` (best for routine runs)
- **Step-by-step** (helpful for debugging or development)
---

## Prerequisites

- **Java 8+** (to compile/run `TranslateServer.java`)
- **Python 3.8+**
- **MongoDB** running locally (`mongodb://localhost:27017`)
- Python packages:
  ```bash
  pip install pymongo tqdm demjson3

### run_pipeline.sh

- To run:
    chmod +x run_pipeline.sh   # make it executable (first time only)
    ./run_pipeline.sh {name_your_run like result4}

- The formatted results will be saved in: 
`example/out/output.json` (unformatted) and `example/out/formatted_results.json` (formatted), then it gets copied over to:
`metric/results/{name_your_run like result4}.json`
and the metric logs in: `metric/utils/logs/{name_your_run like result4}.log`
The metric takes about 5 minuted to execute and calculate for the 2775 test examples

### Step-by-step run

1) Start the Translate Server (Java)
    Compile: javac -cp .:mongodb_unityjdbc_full.jar TranslateServer.java
    Run the server: java -cp .:mongodb_unityjdbc_full.jar TranslateServer
    Keep this terminal open; the server will listen on http://localhost:8082.

2) Collect predicted Mongo queries
Open a new terminal (leave the server running) and run:
python3 collect_mql_preds.py \
  --in out/merged.jsonl \
  --out out/output.json \
  --url http://localhost:8082/translate \
  --method get \
  --debug \
  --probe
Input: out/merged.jsonl
Output: out/output.json
What this does: queries the TranslateServer for each SQL and saves predicted MongoDB queries.
3) Format the results for metrics
python3 formatter.py
Input: out/output.json
Output: out/formatted_results.json
What this does: flattens results into per-NLQ rows with fields:
count, db_id, nlq, SQL, SQL_pred, MQL, MQL_pred.
4) Copy formatted file into the metrics results/ folder
Choose a run name (e.g., my_run_2025_08_22) and copy:
cp -f out/formatted_results.json metric/results/my_run_2025_08_22.json
5) Run metrics
cd metric/utils
python3 metric2.py --file_name my_run_2025_08_22
Reads: ../results/my_run_2025_08_22.json
Logs: ./logs/my_run_2025_08_22_metrics.log
Metrics computed:
    - EM – Exact Match (normalized string equality of MQL)
    - QSM – Query Stage Match (pipeline stage sequence)
    - QFC – Query Fields Coverage (referenced fields set)
    - EX – Execution Accuracy (deep equality of results)
    - EFM – Execution Fields Match (returned field sets)
    - EVM – Execution Value Match (per-document deep equality)
6) Stop the server when done
Go back to the server terminal and press Ctrl+C.
