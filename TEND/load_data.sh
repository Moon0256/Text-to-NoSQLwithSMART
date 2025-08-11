# When in https://github.com/Moon0256/Text-to-NoSQLwithSMART/TEND,  run the following script in the terminal (this will load the flattened json into mongodb)

for db_folder in ./flattened_mongodb_collections/*; do
      dbname=$(basename "$db_folder")
      for json_file in "$db_folder"/*.json; do
         collection=$(basename "$json_file" .json)
         echo "📦 Importing '$collection' into database '$dbname'..."
         ../../mongotools/bin/mongoimport.exe --uri="mongodb://admin:ubco25@localhost:27017/" \
            --db "$dbname" \
            --authenticationDatabase "admin" \
            --collection "$collection" \
            --file "$json_file" \
            --jsonArray
      done
   done
