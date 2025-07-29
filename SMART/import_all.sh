DATA_DIR="../TEND/mongodb_spl3"
ls "$DATA_DIR"

DB_NAME="TEND"

# Loop through all .json files in the directory
for file in "$DATA_DIR"/*.json; do
  # Extract filename without directory and extension
  filename=$(basename "$file" .json)

  # Extract the collection name as the last part after the last underscore
  collection_name="${filename##*_}"

  echo "Importing $file into $DB_NAME.$collection_name ..."

  # Run mongoimport for each file
  mongoimport --db "$DB_NAME" --collection "$collection_name" --file "$file" --jsonArray
done

echo "✅ All files imported into database '$DB_NAME'."
# Final message after all files have been processed




