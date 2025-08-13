from typing import List, Dict
import json
import os

class MongoFieldParser:
    def __init__(self, db_name: str):
        """Initialize the parser with the database name to load its schema."""

        # get the schema file path
        schema_file = "../../TEND/mongodb_schema/" + db_name + ".json"
        # Changed the path, since running it from utils

        with open(schema_file, 'r', encoding='utf-8') as f:
            schema = json.load(f)
        
        # Extract all fields
        self.all_fields = self._extract_schema_fields(schema) if schema else set()
    
    def _extract_schema_fields(self, schema: Dict, prefix: str = "") -> set:
        """Extract all fields from the schema, excluding paths"""
        fields = set()
        
        def process_object(obj: Dict):
            for key, value in obj.items():
                # add the current field name
                fields.add(key)
                
                if isinstance(value, dict):  # handling nested objects
                    process_object(value)
                elif isinstance(value, list) and value:  # processing arrays
                    if isinstance(value[0], dict):
                        process_object(value[0])
        
        process_object(schema)
        return fields
    
    def parse_query(self, query: str) -> List[str]:
        """parse MongoDBquery statement to extract all used fields"""
        found_fields = set()
        
        # Preprocess query string - remove whitespace characters
        query = ' '.join(query.split())
        
        # traverse all known fields and check if they appear in the query
        for field in self.all_fields:
            # check common field reference methods
            if (f'"{field}"' in query or          # "HIRE_DATE"
                f"'{field}'" in query or          # 'HIRE_DATE'
                f"${field}" in query or           # $HIRE_DATE
                f"{field}:" in query or           # HIRE_DATE:
                f".${field}" in query):           # .$HIRE_DATE
                found_fields.add(field)
        
        return sorted(list(found_fields))

def extract_fields(MQL: str, db_name: str) -> List[str]:
    """extract all fields used in a MongoDB query statement."""
    parser = MongoFieldParser(db_name=db_name)
    return parser.parse_query(MQL)

if __name__ == "__main__":  
    # Test query
    query = "db.jobs.aggregate([\n  {\n    $unwind: \"$employees\"\n  },\n  {\n    $match: {\n      \"employees.FIRST_NAME\": {\n        $not: {\n          $regex: \"M\",\n          $options: \"i\"\n        }\n      }\n    }\n  },\n  {\n    $project: {\n      FIRST_NAME: \"$employees.FIRST_NAME\",\n      LAST_NAME: \"$employees.LAST_NAME\",\n      HIRE_DATE: \"$employees.HIRE_DATE\",\n      SALARY: \"$employees.SALARY\",\n      DEPARTMENT_ID: \"$employees.DEPARTMENT_ID\",\n      _id: 0\n    }\n  }\n]);\n"
    

    # Print query
    print(f"Query: {query}")
    fields = extract_fields(query, db_name="hr_1")
    print(f"Found fields: {json.dumps(fields, indent=4)}")
