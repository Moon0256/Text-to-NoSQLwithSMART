#!/usr/bin/env bash
# ^ Use the user's default bash to run this script.

# set -e
# ^ Exit immediately if any command returns a non-zero (error) code.

PORT=8082
JAR_NAME="./mongodb_unityjdbc_full.jar"

COLLECT_SCRIPT="./collect_mql_preds.py"
FORMATTER_SCRIPT="./formatter.py"

MERGED_IN="./out/merged.jsonl"
OUTPUT_JSON="./out/output.json"
FORMATTED_JSON="out/formatted_results.json"

METRIC_UTILS_DIR="../metric/utils"
METRIC_RESULTS_DIR="../metric/results"
METRIC_EXPECTED_BASENAME="results"
# ^ metric2.py reads ../results/<file_name>.json.

if [[ $# -lt 1 ]]; then
  echo "Usage: $0 <result_name_without_ext>"
  # Pass the result name (e.g. run_08_22).
  exit 1
fi

RESULT_NAME="$1"
# The final results file will be: metric/results/${RESULT_NAME}.json

mkdir -p "out" "$METRIC_RESULTS_DIR" "logs"

echo "Compiling TranslateServer.java ..."
javac -cp ".:${JAR_NAME}" TranslateServer.java

echo "Starting TranslateServer on port ${PORT} ..."
# Run the server in the background and redirect output to logs/server.log.
# The "&" means "run in background"; "$!" stores the background process ID (PID).
( java -cp ".:${JAR_NAME}" TranslateServer ) > "logs/server.log" 2>&1 &
JAVA_PID=$!
echo "TranslateServer PID: $JAVA_PID"

trap 'echo "Stopping TranslateServer (PID $JAVA_PID)"; kill "$JAVA_PID" 2>/dev/null || true' EXIT

echo "Waiting for server to be ready at http://localhost:${PORT}/ ..."
for i in {1..30}; do
  if curl -s "http://localhost:${PORT}/" >/dev/null 2>&1 || curl -sI "http://localhost:${PORT}/" >/dev/null 2>&1; then
    echo "Server is up."
    break
  fi
  sleep 1
done

echo "Collecting predictions ..."
python "$COLLECT_SCRIPT" \
  --in "$MERGED_IN" \
  --out "$OUTPUT_JSON" \
  --url "http://localhost:${PORT}/translate" \
  --method get \
  --debug \
  --probe

echo "Formatting results ..."
python "$FORMATTER_SCRIPT"

TARGET_RESULT_PATH="${METRIC_RESULTS_DIR}/${RESULT_NAME}.json"
echo "Copying ${FORMATTED_JSON} -> ${TARGET_RESULT_PATH}"
cp -f "$FORMATTED_JSON" "$TARGET_RESULT_PATH"

echo "Running metrics ..."
cd "$METRIC_UTILS_DIR"

python metric2.py --file_name "$RESULT_NAME"

echo "Pipeline completed successfully. Results in ${TARGET_RESULT_PATH} and metrics output above."
