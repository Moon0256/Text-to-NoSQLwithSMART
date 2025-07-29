import json

input_file = "../TEND/test_SLM_subset.json"
output_txt = "../TEND/test_subset_cleaned.txt"

with open(input_file, "r", encoding="utf-8") as f:
    data = json.load(f)


cleaned_data = []

with open(output_txt, "w", encoding="utf-8") as txt_out:
    for item in data:
        if "MQL" in item:
            mql_raw = item["MQL"]
            try:
                # Convert escaped sequences to actual newlines, tabs, etc.
                mql_clean = bytes(mql_raw, "utf-8").decode("unicode_escape")
                item["MQL_clean"] = mql_clean
                cleaned_data.append(item)

                # Write to .txt for manual inspection
                txt_out.write(f"Record ID: {item.get('record_id', 'N/A')}\n")
                txt_out.write(f"NLQ: {item.get('nlq', 'N/A')}\n")
                txt_out.write("Cleaned MQL:\n")
                txt_out.write(mql_clean + "\n")
                txt_out.write("=" * 60 + "\n\n")

            except Exception as e:
                print(f"Failed to clean MQL for record {item.get('record_id')}: {e}")

print(f"✅ Cleaned MQLs saved to:\n→ {output_txt} (text)\n")