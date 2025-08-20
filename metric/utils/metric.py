import demjson3 as demjson
import json
from dataclasses import dataclass
from pathlib import Path
from pymongo import MongoClient
from tqdm import tqdm
from typing import List, Dict, Tuple
import re
import time
from contextlib import contextmanager
import sys
from contextlib import redirect_stdout, redirect_stderr

from extract_fields import extract_fields
from extract_stages import get_query_stages
from mongosh_exec import MongoShellExecutor

@dataclass
class MetricConfig:
    """Configuration class for evaluation metrics"""
    mongodb_uri: str = 'mongodb://localhost:27017/'
    wrong_examples_path: Path = Path('./wrong_examples_icl.json')
    metrics_list: List[str] = ('EX', 'EM', 'QSM', 'QFC', 'EFM', 'EVM')
    
    # Simplified configuration
    cache_size: int = 1000  
    timeout: int = 30      

class QueryComparator:
    """Query comparator"""
    
    def __init__(self, config: MetricConfig):
        self.client = MongoClient(config.mongodb_uri)
        self.executor = MongoShellExecutor()
        
    def _get_query_result(self, db_id: str, query: str) -> List[Dict]:
        """Execute the query and return the result as a list of dictionaries"""
        result = self.executor.execute_query(db_id, query) #Added get_str=True to return string output because wasn't executing properly earlier
        # clean extra quotes and parse JSON if needed
        if isinstance(result, str):
            result = result.replace('"""', '"')
            try:
                result = json.loads(result)
            except json.JSONDecodeError:
                # if it still fails, try demjson
                try:
                    result = demjson.decode(result)
                except:
                    print(f"Warning: Unable to parse result for query: {query}")
                    result = []
        return result
    
    def _deal_query(self, query: str) -> str:
        """Process the query string removing characters such as carriage returns
        
        This method cleans and normalizes the query string by:
        1. Use strip() to remove leading and trailing whitespace characters
        2. Use a regular expression to replace multiple consecutive whitespace characters with a single space
        
        Parameters:
            query: The input query string
            
        Returns:
            The normalized query string T after processing
        """
        query = re.sub(r'\s+', ' ', query.strip())
        return query

    def _compare_values(self, val1, val2) -> bool:
        """Recursively compares two values for equality supporting nested structures"""
        if isinstance(val1, dict) and isinstance(val2, dict):
            if set(val1.keys()) != set(val2.keys()):
                return False
            return all(self._compare_values(val1[k], val2[k]) for k in val1)
        elif isinstance(val1, (list, tuple)) and isinstance(val2, (list, tuple)):
            if len(val1) != len(val2):
                return False
            return all(self._compare_values(v1, v2) for v1, v2 in zip(val1, val2))
        else:
            return val1 == val2

    def compare(self, query1: str, query2: str, db_id: str) -> Dict[str, int]:
        """compare the execution results and structures of two MongoDB queries"""
        metrics = {metric: 0 for metric in MetricConfig.metrics_list}
        
        print("\n" + "="*50)
        print(f"[DB ID: {db_id}]")
        print(f"TARGET QUERY:\n{query1}")
        print(f"PREDICTION QUERY:\n{query2}")
        # Process the query string and calculate EM
        parsed_query1 = self._deal_query(query1)
        parsed_query2 = self._deal_query(query2)
        metrics['EM'] = parsed_query1 == parsed_query2

        print(f"Exact Match (EM): {metrics['EM']} (Exact string match after normalization)")
        
        # Extract and compare query phase, calculate QSM
        try:
            stages1 = get_query_stages(query=query1)
            stages2 = get_query_stages(query=query2)

            print(f"Extracted Stages from TARGET: {stages1}")
            print(f"Extracted Stages from PREDICTION: {stages2}")

            metrics['QSM'] = stages1 == stages2
        except Exception as e:
            print(f"Error calculating QSM for db_id {db_id}: {str(e)}")
            metrics['QSM'] = 0
            
        # Extract and compare query fields, calculate QFC
        try:
            fields1 = extract_fields(MQL=query1, db_name=db_id)
            fields2 = extract_fields(MQL=query2, db_name=db_id)

            print(f"TARGET fields: {fields1}")
            print(f"PREDICT fields: {fields2}")

            metrics['QFC'] = set(fields1) == set(fields2)
        except Exception as e:
            print(f"Error calculating QFC for db_id {db_id}: {str(e)}")
            metrics['QFC'] = 0
            
        # Execute both queries and compare results, calculate EX, EFM, EVM
        try:
            result1 = self._get_query_result(db_id, query1)
            result2 = self._get_query_result(db_id, query2)
            
            print(f"TARGET execution result:\n{json.dumps(result1, indent=2, ensure_ascii=False)}")
            print(f"PREDICT execution result:\n{json.dumps(result2, indent=2, ensure_ascii=False)}")

            # Compare the entire result using a recursive comparison function
            metrics['EX'] = self._compare_values(result1, result2)
            
            # EFM and EVM are only calculated after successful execution
            target_fields1, target_fields2 = set(), set()
            metrics['EFM'] = 1
            metrics['EVM'] = 1
            
            def get_all_fields(d, fields):
                """Recursively get all fields"""
                if isinstance(d, dict):
                    for k, v in d.items():
                        fields.add(k)
                        get_all_fields(v, fields)
                elif isinstance(d, (list, tuple)):
                    for item in d:
                        get_all_fields(item, fields)

            # Recursively get all fields
            for res1, res2 in zip(result1, result2):
                get_all_fields(res1, target_fields1)
                get_all_fields(res2, target_fields2)
                if not self._compare_values(res1, res2):
                    metrics['EVM'] = 0

            print(f"TARGET result fields: {target_fields1}")
            print(f"PREDICT result fields: {target_fields2}")
        
            if target_fields1 != target_fields2:
                metrics['EFM'] = 0
                
        except Exception as e:
            print(f"Error executing queries for db_id {db_id}: {str(e)}")
            metrics['EX'] = 0
            metrics['EFM'] = 0
            metrics['EVM'] = 0

        print(f"Final metrics for db_id {db_id}: {metrics}") 
        print("="*50 + "\n")

        return metrics

@contextmanager
def timer(name: str):
    start = time.time()
    yield
    print(f"{name} took {time.time() - start:.2f} seconds")

class AccuracyCalculator:
    """Accuracy calculator for MongoDB queries"""
    
    def __init__(self, config: MetricConfig):
        self.config = config
        self.comparator = QueryComparator(config)
        
    def calculate(self, examples: List[Dict], need_print: bool = False, need_save: bool = False) -> Tuple[Dict[str, float], str]:
        """Calculating accuracy"""
        metrics = {metric: 0 for metric in self.config.metrics_list}
        wrong_examples = []
        total_examples = len(examples)
        
        with timer("Processing examples"):
            for example in tqdm(examples, desc="Processing examples"):
                try:
                    example_acc = self.comparator.compare(
                        example['target'],
                        example['prediction'],
                        example['db_id']
                    )
                    
                    for metric, acc in example_acc.items():
                        # Make sure the accumulated score is 0 or 1
                        metrics[metric] += min(1, max(0, acc))
                        
                    if example_acc.get('EX', 1) == 0:
                        formatted_example = self._format_example(example, example_acc)
                        wrong_examples.append(formatted_example)
                        
                except Exception as e:
                    print(f"\nError processing example:")
                    print(f"DB ID: {example['db_id']}")
                    print(f"Error: {str(e)}\n")

        # Calculate the mean using total number of examples
        if total_examples > 0:
            for metric in metrics:
                metrics[metric] = metrics[metric] / total_examples
                # Make sure the final score does not exceed 1 (100%)
                metrics[metric] = min(1.0, metrics[metric])
                
        acc_str = self._format_metrics_string(metrics)
        
        if need_print:
            print(acc_str)
            if wrong_examples:
                print(f"\nTotal errors: {len(wrong_examples)} out of {len(examples)} examples")
        if need_save:
            self._save_wrong_examples(wrong_examples)
            
        return metrics, acc_str
    
    def _format_example(self, example: Dict, acc: Dict) -> Dict:
        """Formatting sample data"""
        return {
            "NLQ": example['NLQ'],
            "db_id": example['db_id'],
            "prediction": example['prediction'],
            "target": example['target'],
            "flag": acc['EX'] == 1
        }
    # Each metric is given either 0 or 1
    
    def _format_metrics_string(self, metrics: Dict[str, float]) -> str:
        """Formatting indicator strings"""
        return f"""
    Exact Match: {metrics['EM']}
    Query Stages Match(QSM): {metrics['QSM']}
    Query Fields Coverage(QFC): {metrics['QFC']}
    Execution Accuracy: {metrics['EX']}
    Execution Fields Match(EFM): {metrics['EFM']}
    Execution Value Match(EVM): {metrics['EVM']}
"""
    
    def _save_wrong_examples(self, wrong_examples: List[Dict]):
        """Save error examples"""
        with open(self.config.wrong_examples_path, "w") as f:
            json.dump(wrong_examples, f, indent=4)

#  python ./src/utils/metric.py

# Main version 1 (worked and was commented on 2024-01-15)
# if __name__ == "__main__":
#     # File name
#     file_name = "result1"
#     # file_name = "test_debug_rag_exec20_gpt"
#     print(f"File name: {file_name}")

#     # Data path
#     # predictions_path = f"./TEND/{file_name}.json"
#     predictions_path = f"../results/{file_name}.json"

#     # Configuration
#     config = MetricConfig(
#         cache_size=10000,  # Increase cache size
#         wrong_examples_path=Path(f'./error_case/{file_name}.json'),
#     )
#     calculator = AccuracyCalculator(config)
    
#     # Loading data
#     with open(predictions_path, 'r', encoding='utf-8') as f:
#         predictions = json.load(f)
    
#     # Reconstructing data format
#     results = [{
#         "db_id": example['db_id'],
#         "NLQ": example['nlq'],
#         "target": example['MQL'],
#         "prediction": example['MQL_pred'],
#     } for example in predictions]

#     # Calculation indicators
#     metric, metric_str = calculator.calculate(results, need_print=True)

if __name__ == "__main__":
    # File name
    file_name = "result1"
    predictions_path = f"../results/{file_name}.json"

    # Open log file (append 'w' to overwrite each run, or 'a' to append)
    log_path = f"./logs/{file_name}_metrics.log"
    Path("./logs").mkdir(exist_ok=True)

    with open(log_path, "w", encoding="utf-8") as log_file:
        # Redirect both stdout and stderr to the file
        with redirect_stdout(log_file), redirect_stderr(log_file):
            print(f"File name: {file_name}")

            config = MetricConfig(
                cache_size=10000,
                wrong_examples_path=Path(f'./error_case/{file_name}.json'),
            )
            calculator = AccuracyCalculator(config)

            # Load predictions
            with open(predictions_path, 'r', encoding='utf-8') as f:
                predictions = json.load(f)

            results = [{
                "db_id": example['db_id'],
                "NLQ": example['nlq'],
                "target": example['MQL'],
                "prediction": example['MQL_pred'],
            } for example in predictions]

            # Run metrics
            metric, metric_str = calculator.calculate(results, need_print=True)

    print(f"Log saved to {log_path}")