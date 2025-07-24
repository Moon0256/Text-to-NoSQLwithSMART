import json
from tqdm import tqdm
import os
from typing import Dict, Any, List, Union
import demjson3 as demjson

text2nosql_system = "You are now the MongoDB natural language interface, responsible for converting user input natural language queries into MongoDB query statements based on the MongoDB database schemas"

text2nosql_instruction = """Given the MongoDB schemas and a natural language query, please convert it into a valid MongoDB query statement."""

MONGODB_SCHEMA_DIR = "../TEND/mongodb_schema"
DATA_DIR = "../TEND"
OUTPUT_DIR = "./SLM_data_cross_domain"

system = "You are now the MongoDB natural language interface, responsible for converting user input natural language queries into MongoDB query statements based on the MongoDB database schemas, and parsing and outputting the features according to user requirements."

instruction = {
    "query_collection":"Given the MongoDB schemas and natural language query, please predict the collection used in the query.",
    "db_fields":"""Given the MongoDB schemas and natural language query, please predict the database fields used in the query.""",
    "alias_fields":"""Given the natural language query, please predict the fields used in the query.""",
    "target_fields":"""Given the natural language query, please predict the fields in the corresponding query results."""
}

# This extracts all fields from MongoDB queries, which is then used by extract_fields(), process_example(), and prepare_training_data() functions.
class MongoFieldParser:
    def __init__(self):
        self.fields = set()
        
    def parse_query(self, query: str) -> list:
        """
        Parse a MongoDB query and extract all fields used in it.
        
        Args:
            query (str): MongoDB query statement
            
        Returns:
            List[str]: List of all fields used in the query
        """
        self.fields.clear()
        
        if ".find(" in query:
            self._parse_find_query(query)
        elif ".aggregate(" in query:
            self._parse_aggregate_query(query)
            
        return sorted(list(self.fields))
    

    def _parse_find_query(self, query: str) -> None:
        """Parsing fields in find queries"""
        args_str = "[" + query.split(".find(", 1)[1].split(")", 1)[0].strip(";") + "]"
        try:
            args = demjson.decode(args_str)
            if args and isinstance(args[0], dict):
                self._extract_fields_from_dict(args[0])
                
            # Handle the projected field (second parameter)
            if len(args) > 1 and isinstance(args[1], dict):
                self._extract_projection_fields(args[1])
        except Exception as e:
            print(f"Error parsing find query: {e}")
    
    def _parse_aggregate_query(self, query: str) -> None:
        """resolving fields in aggregate queries"""
        pipeline_str = query.split(".aggregate(", 1)[1].split(")", 1)[0].strip(";")
        try:
            pipeline = demjson.decode(pipeline_str)
            if isinstance(pipeline, list):
                for stage in pipeline:
                    if isinstance(stage, dict):
                        self._parse_aggregate_stage(stage)
        except Exception as e:
            print(f"Error parsing aggregate query: {e}")

    def _parse_aggregate_stage(self, stage: dict) -> None:
        """analyze each stage in the aggregation pipeline"""
        if not isinstance(stage, dict):
            return
            
        for operator, value in stage.items():
            if operator == "$match":
                if "$expr" in value:
                    self._handle_expr_operator(value["$expr"])
                else:
                    self._extract_fields_from_dict(value)
            elif operator == "$group":
                self._extract_group_fields(value)
            elif operator == "$project":
                self._extract_projection_fields(value)
            elif operator == "$lookup":
                self._extract_lookup_fields(value)
            elif operator == "$unwind":
                # Handling $unwind operators
                if isinstance(value, str):
                    self.fields.add(value.strip("$"))
                elif isinstance(value, dict) and "path" in value:
                    self.fields.add(value["path"].strip("$"))
            elif operator == "$sort":
                # Hnadling $sort operators
                if isinstance(value, dict):
                    for field in value.keys():
                        if not field.startswith("$"):
                            self.fields.add(field)
            elif operator in ["$count", "$limit", "$skip"]:
                pass

    def _handle_expr_operator(self, expr_value: dict) -> None:
        """handling the $expr operator in aggregation pipelines"""
        if isinstance(expr_value, dict):
            for op, value in expr_value.items():
                if isinstance(value, list):
                    for item in value:
                        if isinstance(item, str) and item.startswith("$"):
                            self.fields.add(item.strip("$"))
                        elif isinstance(item, dict):
                            self._extract_fields_from_dict(item)
                elif isinstance(value, str) and value.startswith("$"):
                    self.fields.add(value.strip("$"))
                elif isinstance(value, dict):
                    self._extract_fields_from_dict(value)

    def _extract_fields_from_dict(self, d: dict, parent: str = "") -> None:
        """recursively extract field names from a dictionary"""
        if not isinstance(d, dict):
            return
            
        for key, value in d.items():
            if key == "$expr":
                self._handle_expr_operator(value)
                continue
            elif key.startswith("$"):
                continue
                
            full_key = f"{parent}.{key}" if parent else key
            
            if isinstance(value, dict):
                if all(k.startswith("$") for k in value.keys()):
                    self.fields.add(full_key)
                self._extract_fields_from_dict(value, full_key)
            elif isinstance(value, list):
                for item in value:
                    if isinstance(item, dict):
                        self._extract_fields_from_dict(item, full_key)
            else:
                self.fields.add(full_key)
    
    def _extract_group_fields(self, group_dict: dict) -> None:
        """extract fields from $group stage"""
        for key, value in group_dict.items():
            if key == "_id":
                if isinstance(value, str) and value.startswith("$"):
                    self.fields.add(value[1:])
                elif isinstance(value, dict):
                    for _, field in value.items():
                        if isinstance(field, str) and field.startswith("$"):
                            self.fields.add(field[1:])
            elif isinstance(value, dict):
                for op, field in value.items():
                    if isinstance(field, str) and field.startswith("$"):
                        self.fields.add(field[1:])
    
    def _extract_projection_fields(self, proj_dict: dict) -> None:
        """extracting fields from a projection"""
        for key, value in proj_dict.items():
            if not key.startswith("$"):
                self.fields.add(key)
                if isinstance(value, dict):
                    for op, field in value.items():
                        if isinstance(field, str) and field.startswith("$"):
                            self.fields.add(field[1:])
    
    def _extract_lookup_fields(self, lookup_dict: dict) -> None:
        """extracting fields from $lookup stage"""
        if not isinstance(lookup_dict, dict):
            return
            
        if "localField" in lookup_dict:
            self.fields.add(lookup_dict["localField"])
        if "foreignField" in lookup_dict:
            self.fields.add(lookup_dict["foreignField"])
        if "pipeline" in lookup_dict:
            # processing fields in the pipeline
            pipeline = lookup_dict["pipeline"]
            if isinstance(pipeline, list):
                for stage in pipeline:
                    if isinstance(stage, dict):
                        self._parse_aggregate_stage(stage)

    def _extract_simple_fields(self, value: Union[dict, str]) -> None:
        """extracting simple field references"""
        if isinstance(value, str) and value.startswith("$"):
            self.fields.add(value[1:])
        elif isinstance(value, dict):
            for field in value.keys():
                if not field.startswith("$"):
                    self.fields.add(field)

# This returns all fields used in a MongoDB query, used by prepare_training_data()
def extract_fields(MQL: str) -> list:
    """
    extract all fields used in a MongoDB query.
    
    Args:
        MQL (str): MongoDB query statement
        
    Returns:
        List[str]: List of all fields used in the query
    """
    parser = MongoFieldParser()
    return parser.parse_query(MQL)

# Converts nested dictionaries to markdown format
def dfs_dict_md(d: Dict[str, Any], prefix: str = "", level: int = 0) -> str:
    """
    Recursively traverse a nested dictionary and format it as markdown.
    
    Args:
        d: The nested dictionary to traverse
        prefix: Current path prefix for nested fields
        level: Current nesting level for indentation
    
    Returns:
        Markdown formatted string representation of the schema
    """
    md_lines = []
    indent = "  " * level
    
    for key, value in d.items():
        current_path = prefix
        if isinstance(value, list) and value and isinstance(value[0], dict):
            # Handle nested array of objects
            md_lines.append(f"{indent}- {key} (Array):")
            # Process the first item in the array as it represents the structure
            md_lines.append(dfs_dict_md(value[0], current_path + f"{key}.", level + 1))
        elif isinstance(value, dict):
            # Handle nested object
            md_lines.append(f"{indent}- {key} (Object):")
            md_lines.append(dfs_dict_md(value, current_path + f"{key}.", level + 1))
        else:
            # Handle field with type
            md_lines.append(f"{indent}- {key}: {value}")
    
    return "\n".join(md_lines)

# Loads a schema JSON file and retuns a markdown representation, used by process_example()
def schema_to_markdown(db_id: str) -> str:
    """
    Convert MongoDB schema JSON to markdown format.
    
    Args:
        db_id: Database identifier used in the schema filename
    
    Returns:
        Markdown formatted string of the entire schema
    """
    folder_path = "../TEND/mongodb_schema"
    file_path = os.path.join(folder_path, f"{db_id}.json")
    
    try:
        with open(file_path, "r") as f:
            schema_json = json.load(f)
    except FileNotFoundError:
        return f"Error: Schema file for database '{db_id}' not found at {file_path}"
    except json.JSONDecodeError:
        return f"Error: Invalid JSON in schema file for database '{db_id}'"
    
    md_output = []
    
    # Process each collection in the schema
    for collection, fields in schema_json.items():
        md_output.append(f"### Collection: {collection}")
        md_output.append(dfs_dict_md(fields))
        md_output.append("")  # Add blank line between collections
    
    return "\n".join(md_output)

# extracts all collection names from mongoDB query
def get_collection(query):
    # parsing MQL queries
    col = []
    if 'find' in query:
        collection_name = query.split('.')[1]
        return [collection_name]
    elif 'aggregate' in query:
        collection_name = query.split('.')[1]
        col.append(collection_name)
        pipeline = query.split('aggregate(')[1].split(')')[0]
        pipeline = demjson.decode(pipeline)
        for stage_dict in pipeline:
            if "$lookup" in stage_dict:
                lookup_dict = stage_dict["$lookup"]
                col.append(lookup_dict['from'])
                col = list(set(col))
        col.sort()
        return col

# splits extracted fields into fields present in schema and alias which are not present in schema
def get_alias_fields(db_id:str, fields:list):
    folder_path = "../TEND/mongodb_schema/"
    file_path = os.path.join(folder_path, db_id + ".json")

    with open(file_path, "r") as f:
        schemas_json = json.load(f)

    schemas_str = ""
    db_fields = []
    fields_db = []
    fields_alias = []
    for collection, fields_type in schemas_json.items():

        db_fields.extend(dfs_dict_list(d=fields_type, prefix=""))
    for field in fields:
        if field in db_fields:
            fields_db.append(field)
        else:
            fields_alias.append(field)
    fields_db.sort()
    fields_alias.sort()
    return fields_db, fields_alias

# perform DFS on a nested dictionary to extract all field paths
def dfs_dict_list(d, prefix=""):
    """
    traverse a nested dictionary using depth first search 
    
    :param d: nested dictionary
    :param prefix: key path to the current node
    """
    fields = []
    for key, value in d.items():
        current_path = prefix
        if isinstance(value, list):
            current_path = prefix + f"{key}."
            # if the value is a dictionary, recurse into that sub-dictionary
            for sub_d in value:
                fields.extend(dfs_dict_list(sub_d, current_path))
        else:
            fields.append(current_path + key)

    return fields

# Find output fields in mongoDB query
def get_target_fields(query:str):
    # parsing MQL queries
    target_fields = []
    if 'find' in query:
        query_body = demjson.decode("[" + query.split('find(')[1].split(')')[0] + "]")
        _, query_projection = query_body[0], query_body[1]
        target_fields.extend(query_projection.keys())    
    
    elif 'aggregate' in query:
        pipeline = query.split('aggregate(')[1].split(')')[0]
        pipeline = demjson.decode(pipeline)
        for stage in pipeline:
            if "$project" in stage:
                target_fields = list(stage['$project'].keys())
    
    target_fields.sort()
    return target_fields

# loads schema file for a given db_id
def load_schema(db_id: str) -> Dict[str, Any]:
    """load and parse MongoDB schema file"""
    file_path = os.path.join(MONGODB_SCHEMA_DIR, f"{db_id}.json")
    try:
        with open(file_path, "r") as f:
            return json.load(f)
    except FileNotFoundError:
        raise FileNotFoundError(f"Schema file for database '{db_id}' not found at {file_path}")
    except json.JSONDecodeError:
        raise ValueError(f"Invalid JSON in schema file for database '{db_id}'")

# Main function to prepare task specific input output samples for each NLQ, used by main()
def process_example(example: Dict[str, Any], task: str) -> List[Dict[str, Any]]:
    """process a single example based on the specified task"""
    nlqs = example['nl_queries']
    db_id = example['db_id']
    mql = example['MQL'].strip("\n")
    ref_sql = example['ref_sql'].strip()
    
    try:
        schemas_str = schema_to_markdown(db_id).strip()
    except Exception as e:
        print(f"Error processing schema for db_id {db_id}: {str(e)}")
        return []
    
    output = ""
    parser = MongoFieldParser()
    fields = parser.parse_query(mql)
    if task == "text2nosql":
        system_prompt = text2nosql_system
        instruction_prompt = text2nosql_instruction
        output = mql
    elif task == "query_collection":
        system_prompt = system
        instruction_prompt = instruction['query_collection']
        output = ", ".join(get_collection(mql))
    elif task == "alias_fields":
        system_prompt = system
        instruction_prompt = instruction['alias_fields']
        fields_db, fields_alias = get_alias_fields(db_id, fields)
        output = ", ".join(fields_alias)
    elif task == "target_fields":
        system_prompt = system
        instruction_prompt = instruction['target_fields']
        output = ", ".join(get_target_fields(mql))
    elif task == "db_fields":
        system_prompt = system
        instruction_prompt = instruction['db_fields']
        fields_db, fields_alias = get_alias_fields(db_id, fields)
        output = ", ".join(fields_db)
    else:
        raise ValueError(f"Invalid task: {task}")

    processed_examples = []
    for idx, nlq in enumerate(nlqs):
        input_str = f"""## Natural Language Query: `{nlq}`

## Database schemas
{schemas_str}"""
        
        io_sample = {
            "record_id": f"{example['record_id']}_{idx}",
            "db_id": db_id,
            "preference_type": task,
            "system": system_prompt,
            "instruction": instruction_prompt,
            "input": input_str,
            "output": output,
            "history": []
        }
        processed_examples.append(io_sample)
    
    return processed_examples

# creates train_SLM_prediction_data.json for embedding based retrieval, extracts and stores all key componenets using train.json
def prepare_training_data():
    with open("../TEND/train.json", "r", encoding="utf-8") as f:
        train_data = json.load(f)
    
    parser = MongoFieldParser()
    train_data_dict = {}
    for example in train_data:
        fields = parser.parse_query(example['MQL'])
        fields_db, fields_alias = get_alias_fields(example['db_id'], fields)
        fields_db = ", ".join(fields_db)
        fields_alias = ", ".join(fields_alias)
        target_fields = get_target_fields(example['MQL'])
        target_fields = ", ".join(target_fields)
        query_collection = get_collection(example['MQL'])
        query_collection = ", ".join(query_collection)
        for idx, nlq in enumerate(example['nl_queries']):
            record_id = f"{example['record_id']}_{idx}"
            train_data_dict[record_id] = {
                "record_id": record_id,
                "db_id": example['db_id'],
                "nlq": nlq,
                "ref_sql": example['ref_sql'],
                "MQL": example['MQL'],
                "fields_db": fields_db,
                "fields_alias": fields_alias,
                "target_fields": target_fields,
                "query_collection": query_collection
            }

    train_data_list = list(train_data_dict.values())
    with open("../TEND/train_SLM_prediction.json", "w", encoding="utf-8") as f:
        json.dump(train_data_list, f, ensure_ascii=False, indent=4)

def prepare_test_data():
    with open("../TEND/test.json", "r", encoding="utf-8") as f:
        test_data = json.load(f)
    
    parser = MongoFieldParser()
    test_data_dict = {}
    for example in test_data:
        fields = parser.parse_query(example['MQL'])
        fields_db, fields_alias = get_alias_fields(example['db_id'], fields)
        fields_db = ", ".join(fields_db)
        fields_alias = ", ".join(fields_alias)
        target_fields = get_target_fields(example['MQL'])
        target_fields = ", ".join(target_fields)
        query_collection = get_collection(example['MQL'])
        query_collection = ", ".join(query_collection)
        for idx, nlq in enumerate(example['nl_queries']):
            record_id = f"{example['record_id']}_{idx}"
            test_data_dict[record_id] = {
                "record_id": record_id,
                "db_id": example['db_id'],
                "nlq": nlq,
                "ref_sql": example['ref_sql'],
                "MQL": example['MQL'],
                "fields_db": fields_db,
                "fields_alias": fields_alias,
                "target_fields": target_fields,
                "query_collection": query_collection
            }

    test_data_list = list(test_data_dict.values())
    with open("../TEND/test_SLM_prediction.json", "w", encoding="utf-8") as f:
        json.dump(test_data_list, f, ensure_ascii=False, indent=4)

# Loops over all tasks (test2nosql, query_collection, alias_fields, target_fields, db_fields) and all modes (train, test), reads the data and runs process_example() on each example
# Saves results in ./SLM_data_cross_domain/{mode}/{task}.json - Prompt format data for training SLM
def main():
    """main function"""
    tasks = ["text2nosql", "query_collection", "alias_fields", "target_fields", "db_fields"]
    modes = ["train", "test"]
    
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    for task in tasks:
        for mode in modes:
            input_file = os.path.join(DATA_DIR, f"{mode}.json")
            output_dir = os.path.join(OUTPUT_DIR, mode)
            os.makedirs(output_dir, exist_ok=True)
            output_file = os.path.join(output_dir, f"{task}.json")
            
            try:
                with open(input_file, "r", encoding="utf-8") as f:
                    data = json.load(f)
            except Exception as e:
                print(f"Error reading {input_file}: {str(e)}")
                continue
                
            processed_data = []
            for example in tqdm(data, desc=f"Processing {mode} data for {task}"):
                processed_data.extend(process_example(example, task))
            
            try:
                with open(output_file, "w", encoding="utf-8") as f:
                    json.dump(processed_data, f, indent=4, ensure_ascii=False)
                print(f"Successfully saved {len(processed_data)} examples to {output_file}")
            except Exception as e:
                print(f"Error writing to {output_file}: {str(e)}")

if __name__ == "__main__":
    main()
    prepare_training_data()
    prepare_test_data()
