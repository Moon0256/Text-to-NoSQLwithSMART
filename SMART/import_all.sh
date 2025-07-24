mkdir -p ../TEND/mongodb_data_split

for file in ../TEND/mongodb_data/*.json; do
  base=$(basename "$file" .json)

  keys=$(jq -r 'keys[]' "$file")
  for key in $keys; do
    out="../TEND/mongodb_data_split/${base}_${key}.json"
    jq ".$key" "$file" > "$out"
    echo "Importing $out into collection '$key'..."
    mongoimport --db TEND --collection "$key" --file "$out" --jsonArray --drop
  done
done


