import subprocess
import json
import os
from typing import Union, List, Dict
from datetime import datetime
from tqdm import tqdm
import shutil
import platform

class MongoShellExecutor:
    def __init__(self, connection_string: str = "mongodb://localhost:27017", output_dir: str = "output"):
        self.connection_string = connection_string
        self.mongosh_path = self._get_mongosh_path()
        self.output_dir = output_dir
        
        # create output directory
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
        
        # test connection
        self._test_connection()

    def _get_mongosh_path(self) -> str:
        """get the path to the mongosh executable file"""

        for binary in ["mongosh", "mongo"]:
            path = shutil.which(binary)
            if path:
                print(f"Using MongoDB shell at: {path}")
                return path
        # check multiple possible paths
        possible_paths = [
        r"C:\Program Files\MongoDB\Server\7.0\bin\mongosh.exe",
        r"C:\Program Files\MongoDB\Server\6.0\bin\mongosh.exe",
        r"C:\Program Files\MongoDB\Server\5.0\bin\mongosh.exe",
        r"C:\Program Files\MongoDB\Server\4.4\bin\mongo.exe",
        r"C:\Program Files\MongoDB\Server\4.2\bin\mongo.exe",
        r"D:\MongoDB\Server\7.0\bin\mongosh.exe",
        r"D:\MongoDB\Server\6.0\bin\mongosh.exe",
        r"D:\MongoDB\Server\5.0\bin\mongosh.exe",
        r"D:\MongoDB\Server\4.4\bin\mongo.exe",
        r"D:\MongoDB\Server\4.2\bin\mongo.exe"
        ]   
        
        for path in possible_paths:
            if os.path.exists(path):
                print(f"Found MongoDB shell at: {path}")
                return path
            
        raise FileNotFoundError("Could not find 'mongosh' or 'mongo'. Make sure MongoDB is installed and added to your system PATH.")
    
    def _format_query(self, query: str) -> str:
        """formatting query strings"""
        query = query.strip().rstrip(';')
        
        if ('.find(' in query or '.aggregate(' in query) and '.toArray()' not in query:
            query += '.toArray()'
            
        return query

    def _save_to_json(self, data: Union[List, Dict], filename: str = None) -> str:
        """save the results to a json file"""
        if filename is None:
            # Generate filename with timestamp
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"query_result_{timestamp}.json"
        
        filepath = os.path.join(self.output_dir, filename)
        
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
            
        return filepath

    # def execute_query(self, db_name: str, query: str, output_file: str = None, timeout: int = 30, get_str: bool = False) -> List[Dict]:
        
        # try:
        #     formatted_query = self._format_query(query)
        #     js_command = f'''
        #         try {{
        #             db = connect("{self.connection_string}").getSiblingDB("{db_name}");
        #             result = {formatted_query};
        #             printjson(result);
        #         }} catch (e) {{
        #             print("QUERY_ERROR: " + e.message);
        #             quit(1);
        #         }}
        #     '''
            
        #     startupinfo = None
        #     if platform.system() == "Windows":
        #         startupinfo = subprocess.STARTUPINFO()
        #         startupinfo.dwFlags |= subprocess.STARTF_USESHOWWINDOW
            
        #     result = subprocess.run(
        #         [self.mongosh_path, "--quiet", "--eval", js_command],
        #         capture_output=True,
        #         text=True,
        #         timeout=timeout,
        #         encoding='utf-8',
        #         errors='replace',
        #         startupinfo=startupinfo
        #     )
            
        #     if result.stderr:
        #         print(f"Query error: {result.stderr}")
        #         return []
            
        #     output = result.stdout.strip()
        #     if not output:
        #         print("The query result is empty")
        #         return []
            
        #     if "QUERY_ERROR" in output:
        #         print(f"Query execution error: {output}")
        #         return []
            
        #     try:
        #         # Handling MongoDB special types
        #         special_types = [
        #             ('ObjectId\\(([^)]+)\\)', r'"ObjectId(\1)"'),
        #             ('ISODate\\(([^)]+)\\)', r'"ISODate(\1)"'),
        #             ('NumberLong\\(([^)]+)\\)', r'\1'),
        #             ('NumberDecimal\\(([^)]+)\\)', r'\1'),
        #             ('Timestamp\\(([^)]+)\\)', r'"Timestamp(\1)"'),
        #             ('BinData\\(([^)]+)\\)', r'"BinData(\1)"'),
        #             ('DBRef\\(([^)]+)\\)', r'"DBRef(\1)"'),
        #             ('NumberInt\\(([^)]+)\\)', r'\1'),
        #             ('Date\\(([^)]+)\\)', r'"Date(\1)"')
        #         ]
                
        #         import re
        #         for pattern, replacement in special_types:
        #             output = re.sub(pattern, replacement, output)
                    
        #         data = json.loads(output)
        #         if not isinstance(data, list):
        #             data = [data]
                    
        #         # Verify the converted data
        #         for item in data:
        #             if not isinstance(item, (dict, list)):
        #                 print(f"Warning: Unexpected data item type - {type(item)}")
                        
        #         return data
                
        #     except json.JSONDecodeError as e:
        #         if get_str:
        #             out_str = f"Error in Executing Query and Transforming result into JSON: `{str(e)}`"
        #             return out_str
        #         else:
        #             print(f"JSON Parsing error: {str(e)}")
        #             print(f"Error location: {e.pos}")
        #             print(f"error line: {e.lineno}, List: {e.colno}")
        #         print(f"Original output snippet: {output[max(0, e.pos-50):e.pos+50]}")
        #         return []
                
        #     except Exception as e:
        #         if get_str:
        #             out_str = f"Error in Executing Query and Transforming result into JSON: `{str(e)}`"
        #             return out_str
        #         else:
        #             print(f"Data conversion error: {str(e)}")
        #             print(f"Raw output: {output[:200]}...")  # Only show the first 200 characters
        #         return []
                
        # except subprocess.TimeoutExpired:
        #     if get_str:
        #         out_str = f"Query Timeout: `{timeout} seconds`"
        #         return out_str
        #     else:
        #         print(f"Query Timeout (>{timeout} seconds)")
        #     return []
        # except Exception as e:
        #     if get_str:
        #         out_str = f"Error in Executing Query and Transforming result into JSON: `{str(e)}`"
        #         return out_str
        #     else:
        #         print(f"An error occurred while executing the query: {str(e)}")
        #     return []


    def execute_query(self, db_name: str, query: str, output_file: str = None, timeout: int = 30, get_str: bool = False) -> List[Dict]:
        try:
            formatted_query = self._format_query(query)
        
            # Hardcode database to "TEND"
            fixed_db = "TEND"
        
            js_command = f'''
                try {{
                    db = connect("{self.connection_string}").getSiblingDB("{fixed_db}");
                    result = {formatted_query};
                    printjson(result);
                }} catch (e) {{
                    print("QUERY_ERROR: " + e.message);
                    quit(1);
                }}
            '''
        
            result = subprocess.run(
                [self.mongosh_path, "--quiet", "--eval", js_command],
                capture_output=True,
                text=True,
                timeout=timeout,
                encoding='utf-8',
                errors='replace'
            )
        
            if result.stderr:
                print(f"Query error: {result.stderr}")
                return []
        
            output = result.stdout.strip()
            if not output:
                print("The query result is empty")
                return []
        
            if "QUERY_ERROR" in output:
                print(f"Query execution error: {output}")
                return []
        
            try:
                import re
                special_types = [
                    ('ObjectId\\(([^)]+)\\)', r'"ObjectId(\1)"'),
                    ('ISODate\\(([^)]+)\\)', r'"ISODate(\1)"'),
                    ('NumberLong\\(([^)]+)\\)', r'\1'),
                    ('NumberDecimal\\(([^)]+)\\)', r'\1'),
                    ('Timestamp\\(([^)]+)\\)', r'"Timestamp(\1)"'),
                    ('BinData\\(([^)]+)\\)', r'"BinData(\1)"'),
                    ('DBRef\\(([^)]+)\\)', r'"DBRef(\1)"'),
                    ('NumberInt\\(([^)]+)\\)', r'\1'),
                    ('Date\\(([^)]+)\\)', r'"Date(\1)"')
                ]
                for pattern, replacement in special_types:
                    output = re.sub(pattern, replacement, output)
            
                data = json.loads(output)
                if not isinstance(data, list):
                    data = [data]
                return data

            except json.JSONDecodeError as e:
                if get_str:
                    return f"Error in Executing Query and Transforming result into JSON: `{str(e)}`"
                else:
                    print(f"JSON error: {str(e)}")
                    return []
        
        except subprocess.TimeoutExpired:
            if get_str:
                return f"Query Timeout: `{timeout} seconds`"
            else:
                print(f"Query Timeout (>{timeout} seconds)")
                return []
        except Exception as e:
            if get_str:
                return f"Error in Executing Query and Transforming result into JSON: `{str(e)}`"
            else:
                print(f"Execution error: {str(e)}")
                return []


    def execute_script(self, db_name: str, script_path: str) -> str:
        """Execute MongoDB script file"""
        try:
            command = [self.mongosh_path, db_name, script_path]
            result = subprocess.run(
                command,
                capture_output=True,
                text=True,
                check=True
            )
            return result.stdout
        except subprocess.CalledProcessError as e:
            print(f"Error while executing script: {e.stderr}")
            raise

    def _test_connection(self):
        """Testing the database connection"""
        try:
            print(f"Trying to connect to MongoDB: {self.connection_string}")
            js_command = f'''
                try {{
                    db = connect("{self.connection_string}");
                    print("CONNECTION_SUCCESS");
                }} catch (e) {{
                    print("CONNECTION_ERROR: " + e.message);
                    quit(1);
                }}
            '''
            
            # Run commands using UTF-8 encoding
            if platform.system() == "Windows":
                startupinfo = subprocess.STARTUPINFO()
                startupinfo.dwFlags |= subprocess.STARTF_USESHOWWINDOW
            else:
                startupinfo = None

            
            result = subprocess.run(
                [self.mongosh_path, "--quiet", "--eval", js_command],
                capture_output=True,
                text=True,
                timeout=5,
                encoding='utf-8',
                errors='replace',
                startupinfo=startupinfo
            )
            
            if result.stderr:
                print(f"Connection error output: {result.stderr}")
                raise Exception(f"MongoDB Connection Error: {result.stderr}")
                
            if result.stdout:
                print(f"Connection output: {result.stdout}")
            
            if "CONNECTION_SUCCESS" not in result.stdout:
                raise Exception("MongoDB Connection Failed")
                
            print("MongoDB test successful!")
            
        except Exception as e:
            print(f"MongoDB Connection test failed: {str(e)}")
            print("\nPlease make sure: ")
            print("1. MongoDB service has been started（net start MongoDB）")
            print("2. Port 27017 is not occupied")
            print("3. A firewall is not blocking the connection")
            print(f"4. MongoDB path is correct: {self.mongosh_path}")
            raise

def main():
    try:
        executor = MongoShellExecutor(
            connection_string="mongodb://localhost:27017/?connectTimeoutMS=5000",
            output_dir="query_results"
        )
        
        # Reading test data
        test_file = "./TEND/test_debug_rag_exec20_deepseekv3_ori.json"
        print(f"\nReading test file: {test_file}")
        
        with open(test_file, "r", encoding="utf-8") as f:
            data = json.load(f)
        
        print(f"Successfully loaded test data，total {len(data)} records")
        
        for i, example in tqdm(enumerate(data, 1), total=len(data)):
            try:
                # print(f"\nExecute query {i}/{len(data)}:")
                # print(f"Database: {example['db_id']}")
                # print(f"Query: {example['MQL']}")
                
                result = executor.execute_query(
                    example["db_id"], 
                    example['MQL_debug_exec'],
                    get_str=True
                )
                
                if isinstance(result, str):
                    print(f"Query results: {result}")
                # else:
                #     print(f"Query results: {json.dumps(result, ensure_ascii=False, indent=2)}")
                
            except Exception as e:
                print(f"Processing records {example.get('record_id', i)} Error: {str(e)}")
                continue
            
    except Exception as e:
        print(f"Program Execution Error: {str(e)}")

if __name__ == "__main__":
    main()
