import os
import json

# === Configuration ===
input_dir = "../TEND/mongodb_data"         # Folder containing your original JSON files
output_dir = "../TEND/mongodb_spl3"        # Folder to save the split JSON files

# Create the output directory if it doesn't exist
os.makedirs(output_dir, exist_ok=True)

# Loop through each file in the input directory
for filename in os.listdir(input_dir):
    if filename.endswith(".json"):
        input_path = os.path.join(input_dir, filename)

        # Open and parse the JSON file
        with open(input_path, 'r', encoding='utf-8') as f:
            data = json.load(f)

        # Use base filename (without .json) to prefix split files
        base = os.path.splitext(filename)[0]

        # Each top-level key (e.g., "pilot", "Ship", etc.) becomes its own file
        for key, value in data.items():
            # Add both the source filename and key to the output name
            output_filename = f"{base}_{key}.json"
            output_path = os.path.join(output_dir, output_filename)

            # Write the value (list of records) to the new file
            with open(output_path, 'w', encoding='utf-8') as out_f:
                json.dump(value, out_f, indent=2)

            print(f"Created: {output_path}")
