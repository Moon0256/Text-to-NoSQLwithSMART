#!/usr/bin/env python3                  # Shebang: allow running as an executable script on Unix-like systems with Python 3
"""
Fast, instrumented metrics for MongoDB query evaluation.

What changed vs. your original:
- Single global MongoClient reused across all examples.
- PyMongo "fast path" for aggregate() and find() queries (no mongosh subprocess).
- LRU cache for execution results to avoid re-running identical queries.
- Bounded preview printing to avoid spending time formatting huge JSON.
- Lightweight timing breakdown to pinpoint bottlenecks.

Metrics computed (same semantics):
- EM  : exact string match (after whitespace normalization)
- QSM : query stage sequence match
- QFC : query fields coverage set equality
- EX  : deep result equality (structure + values)
- EFM : result fields equality
- EVM : per-document value equality (1 only if every document equals partner)
"""                                    # Module docstring: high-level description and the metric definitions

import json                             # Standard lib: JSON encoding/decoding
import re                               # Regular expressions used for parsing / normalization
import time                             # Timing utilities for profiling
from dataclasses import dataclass       # Dataclass for clean configuration container
from pathlib import Path                # Filesystem path handling
from typing import List, Dict, Tuple, Any  # Type hints for better clarity / IDE support
from functools import lru_cache         # Memoization decorator for caching expensive function results
from collections import defaultdict     # Dict subclass with default factory, used for timing buckets
from contextlib import contextmanager, redirect_stdout, redirect_stderr  # Context managers for timing and log redirection

import demjson3 as demjson              # More lenient JSON parser (handles single quotes / trailing commas, etc.)
from pymongo import MongoClient         # PyMongo client (native driver, faster than shell for exec)
from tqdm import tqdm                   # Progress bar for loop visibility

# Your existing utilities
from extract_fields import extract_fields    # Custom utility: extracts field names from MQL
from extract_stages import get_query_stages  # Custom utility: extracts stage sequence from MQL
from mongosh_exec import MongoShellExecutor  # Custom executor that uses mongosh as a subprocess fallback


# ----------------------------
# Config and simple utilities
# ----------------------------

@dataclass
class MetricConfig:
    """Configuration for evaluation."""
    mongodb_uri: str = 'mongodb://localhost:27017/'             # URI to MongoDB server
    wrong_examples_path: Path = Path('./wrong_examples_icl.json')# Where to dump wrong examples
    metrics_list: List[str] = ('EX', 'EM', 'QSM', 'QFC', 'EFM', 'EVM')  # Which metrics to compute/aggregate

    # Tunables
    cache_size: int = 2048                         # LRU cache size for execution results (not used directly; see @lru_cache below)
    preview_chars: int = 1500                      # Bound how much of results we print to logs to avoid huge dumps
    allow_disk_use: bool = True                    # Pass allowDiskUse to aggregate() for large pipelines

    log_exec_field_details: bool = True            # If True, log field-path sets and sample values from exec results
    value_samples_per_field: int = 3               # For each field path, how many sample values to show
    max_logged_fields: int = 50                    # Cap the number of field paths printed to avoid spam


def _norm_ws(s: str) -> str:
    """Normalize whitespace to a single space, strip ends (for EM + cache keys)."""
    #return re.sub(r'\s+', '', (s or '').strip())  # Replace any whitespace run with a single space; handle None safely
    return re.sub(r'\s+', ' ', (s or '').strip()) # Replace any whitespace run with a single space; handle None safely

def _preview_blob(obj: Any, max_len: int) -> str:
    """Bounded string preview to avoid time-costly pretty-prints."""
    try:
        s = json.dumps(obj, ensure_ascii=False)    # Try to JSON-serialize the object
    except Exception:
        s = str(obj)                               # Fallback to str() if not JSON-serializable
    return s[:max_len] + (' …<truncated>' if len(s) > max_len else '')  # Truncate with ellipsis if too long


def _iter_field_paths(obj, prefix=""):
    """
    Yield dotted field paths (e.g., 'a.b.c') for all nested keys in dicts/lists.
    For lists/tuples we recurse into items without adding numeric indices,
    since indices aren't stable; we care about field *names* only.
    """
    if isinstance(obj, dict):                      # If it's a dict, iterate keys/values
        for k, v in obj.items():
            key = f"{prefix}.{k}" if prefix else k # Build dotted path
            yield key                              # Yield this path
            yield from _iter_field_paths(v, key)   # Recurse into the value with updated prefix
    elif isinstance(obj, (list, tuple)):           # If it's a list/tuple, recurse into elements
        for item in obj:
            yield from _iter_field_paths(item, prefix)  # Do not add numeric index to the path (names only)


def _collect_fields_and_values(results, max_samples=3, max_chars=200):
    """
    From a list of result documents, return:
      - paths: set of dotted field paths across all docs
      - samples: dict[path] -> list of up to `max_samples` stringified sample values
    Values are stringified and truncated to `max_chars` to keep logs compact.
    """
    paths = set()                                  # Accumulate unique field paths
    samples = {}                                   # Map: path -> list of sample stringified values

    def _add_sample(path, val):
        s = _preview_blob(val, max_chars)          # Stringify & truncate each value for logging
        lst = samples.setdefault(path, [])         # Get/create list for this path
        if s not in lst:                           # Avoid duplicate samples
            if len(lst) < max_samples:             # Respect cap per field
                lst.append(s)                      # Add sample

    def _walk(obj, prefix=""):
        if isinstance(obj, dict):                  # Traverse dicts
            for k, v in obj.items():
                key = f"{prefix}.{k}" if prefix else k
                paths.add(key)                     # Record field path
                _add_sample(key, v)                # Record sample value at this path
                _walk(v, key)                      # Recurse into value
        elif isinstance(obj, (list, tuple)):       # Traverse sequences
            for item in obj:
                _walk(item, prefix)                # Keep the same path prefix
        else:
            # leaf value at current prefix (prefix can be empty if root is a scalar)
            if prefix:                             # Only record if we have a path
                _add_sample(prefix, obj)           # Record leaf sample

    # Iterate all docs (handle both a single dict or a list of docs)
    if isinstance(results, (list, tuple)):
        for doc in results:
            _walk(doc, "")
    else:
        _walk(results, "")

    return paths, samples                          # Return the set of paths and sample values


@contextmanager
def timer(name: str):
    """Context timer used around the main loop."""
    t0 = time.time()                               # Capture start wall-time
    yield                                          # Execute the context block
    print(f"{name} took {time.time() - t0:.2f} seconds")  # On exit, print elapsed time


# ----------------------------
# Timing decorators (profiling)
# ----------------------------

_TIMINGS = defaultdict(float)                      # Accumulate per-label timing across calls

def timed(label: str):
    """Decorator to accumulate time spent in labeled sections."""
    def deco(fn):
        def wrapper(*args, **kwargs):
            t0 = time.perf_counter()               # High-resolution timer start
            out = fn(*args, **kwargs)              # Invoke wrapped function
            _TIMINGS[label] += time.perf_counter() - t0  # Add elapsed to label bucket
            return out                             # Return original result
        return wrapper
    return deco


# ----------------------------
# PyMongo fast-path parsing
# ----------------------------

# Matches: db.collection.aggregate([...])
AGG_RE = re.compile(
    r'^db\.([A-Za-z0-9_]+)\.aggregate\(\s*(\[.*\])\s*\)\s*;?\s*$',
    re.DOTALL                                    # DOTALL so '.' matches newlines (pipelines often span lines)
)

# Matches: db.collection.find({filter}[, {projection}])
FIND_RE = re.compile(
    r'^db\.([A-Za-z0-9_]+)\.find\(\s*'          # Start with db.<coll>.find(
    r'(\{.*?\})'                                 # Capture group 1: filter object (non-greedy)
    r'(?:\s*,\s*(\{.*?\}))?'                     # Optional group 2: projection object (non-greedy)
    r'\s*\)\s*;?\s*$',                           # Close paren, optional semicolon, end
    re.DOTALL
)


def _maybe_json_load(text: str):
    """Try json.loads, then demjson as a fallback for non-strict JSON."""
    try:
        return json.loads(text)                   # Strict JSON first (fastest/cleanest)
    except json.JSONDecodeError:
        return demjson.decode(text)               # Fallback: demjson handles lenient JSON


def _try_parse_aggregate(mql: str):
    """Parse 'db.coll.aggregate([...])' → (collection, pipeline:list) or (None, None)."""
    m = AGG_RE.match(mql.strip())                 # Try to match aggregate() shape
    if not m:
        return None, None                         # Not an aggregate call
    coll = m.group(1)                             # Extract collection name
    pipeline_text = m.group(2)                    # Extract raw pipeline text (JSON array)
    try:
        pipeline = _maybe_json_load(pipeline_text)# Parse pipeline text to Python list/dicts
        if not isinstance(pipeline, list):        # Validate list
            return None, None
        return coll, pipeline                     # Success: return collection & pipeline
    except Exception:
        return None, None                         # On any parse error, signal failure


def _try_parse_find(mql: str):
    """Parse 'db.coll.find({...}, {...})' → (collection, filter:dict, proj:dict|None) or (None, None, None)."""
    m = FIND_RE.match(mql.strip())                # Try to match find() shape
    if not m:
        return None, None, None                   # Not a find call
    coll = m.group(1)                             # Extract collection name
    filter_text = m.group(2)                      # Extract filter JSON
    proj_text = m.group(3)                        # Extract optional projection JSON
    try:
        filt = _maybe_json_load(filter_text)      # Parse filter to dict
        proj = _maybe_json_load(proj_text) if proj_text else None  # Parse projection if present
        if not isinstance(filt, dict):            # Validate filter is dict
            return None, None, None
        if proj is not None and not isinstance(proj, dict):  # Validate projection if present
            proj = None
        return coll, filt, proj                   # Success: return parsed pieces
    except Exception:
        return None, None, None                   # On parse error, signal failure


# ----------------------------
# Core comparator
# ----------------------------

class QueryComparator:
    """Compares two MQL strings with structural and execution-based metrics."""

    def __init__(self, config: MetricConfig):
        self.config = config                      # Keep config for later use
        # One MongoClient for the whole run — avoids reconnect costs per example.
        self.client = MongoClient(config.mongodb_uri)
        # Keep your shell executor, but we only use it as a fallback.
        self.executor = MongoShellExecutor()

    # Freeze/thaw utilities make cached results hashable for lru_cache
    @staticmethod
    def _freeze(obj: Any):
        if isinstance(obj, dict):                 # For dict: convert to sorted tuple of (key, frozen(value))
            return tuple(sorted((k, QueryComparator._freeze(v)) for k, v in obj.items()))
        if isinstance(obj, list):                 # For list: convert to tuple of frozen elements
            return tuple(QueryComparator._freeze(v) for v in obj)
        if isinstance(obj, tuple):                # For tuple: recursively freeze items
            return tuple(QueryComparator._freeze(v) for v in obj)
        return obj                                # For scalars: return as-is

    @staticmethod
    def _thaw(obj: Any):
        if isinstance(obj, tuple):                # If tuple, it might represent dict or list
            # Heuristic: dict-like if members are (str, value) pairs
            if all(isinstance(i, tuple) and len(i) == 2 and isinstance(i[0], str) for i in obj):
                return {k: QueryComparator._thaw(v) for k, v in obj}  # Convert back to dict
            return [QueryComparator._thaw(v) for v in obj]            # Else convert back to list
        return obj                                # Scalars unchanged

    def _norm_cache_key(self, db_id: str, query: str) -> str:
        # Use normalized whitespace to de-duplicate semantically identical query text.
        return f"{db_id}||{_norm_ws(query)}"      # Key: "<db>||<normalized query>"

    @lru_cache(maxsize=2048)
    @timed("exec")                                # Measure time spent executing (fast-path or shell)
    def _cached_exec(self, cache_key: str) -> tuple:
        """
        Cached execution path that:
        1) Tries PyMongo fast path for aggregate/find.
        2) Falls back to mongosh for anything else.
        Returns a FROZEN (hashable) structure to fit lru_cache.
        """
        db_id, query = cache_key.split("||", 1)   # Split cache key back into db + query
        db = self.client[db_id]                   # Get DB handle from global client

        # --- Fast path #1: aggregate([...]) ---
        coll, pipeline = _try_parse_aggregate(query)
        if coll and pipeline is not None:         # If it parses as aggregate()
            try:
                docs = list(db[coll].aggregate(   # Run natively with PyMongo (fast)
                    pipeline,
                    allowDiskUse=self.config.allow_disk_use
                ))
                return QueryComparator._freeze(docs)  # Freeze results for caching
            except Exception:
                # Fall through to shell if pipeline/parse failed at runtime
                pass

        # --- Fast path #2: find(filter, projection) ---
        coll, filt, proj = _try_parse_find(query)
        if coll and filt is not None:             # If it parses as find()
            try:
                cur = db[coll].find(filt, proj)   # Run natively with PyMongo
                docs = list(cur)                  # Materialize cursor
                return QueryComparator._freeze(docs)
            except Exception:
                # Fall through to shell if something is unsupported
                pass

        # --- Fallback: use mongosh executor (likely slower) ---
        result = self.executor.execute_query(db_id, query)  # Use shell-based executor
        if isinstance(result, str):               # If result is a raw string, attempt to parse into Python objects
            result = result.replace('"""', '"')   # Normalize triple quotes if present
            try:
                result = json.loads(result)
            except json.JSONDecodeError:
                try:
                    result = demjson.decode(result)  # Lenient fallback
                except Exception:
                    result = []                   # If parsing fails, default to empty

        return QueryComparator._freeze(result)    # Freeze before returning to satisfy lru_cache

    def _get_query_result(self, db_id: str, query: str) -> List[Dict]:
        """
        Public entry: returns a thawed (normal) Python structure.
        (Executes via cache; repeated queries are free.)
        """
        frozen = self._cached_exec(self._norm_cache_key(db_id, query))  # Execute through cached path
        return QueryComparator._thaw(frozen)        # Convert frozen structure back to normal Python (lists/dicts)

    @staticmethod
    def _deep_equal(a: Any, b: Any) -> bool:
        """Deep equality for nested dicts/lists/tuples."""
        if isinstance(a, dict) and isinstance(b, dict):
            if set(a.keys()) != set(b.keys()):      # Dict keys must match
                return False
            return all(QueryComparator._deep_equal(a[k], b[k]) for k in a)  # Recurse per key
        if isinstance(a, (list, tuple)) and isinstance(b, (list, tuple)):
            if len(a) != len(b):                    # Lengths must match
                return False
            return all(QueryComparator._deep_equal(x, y) for x, y in zip(a, b))  # Recurse per element
        return a == b                                # Scalar equality

    def compare(self, query_gold: str, query_pred: str, db_id: str) -> Dict[str, int]:
        """Compute all metrics for a pair of MQL strings on the given db."""
        # Initialize all metric flags to 0
        metrics = {m: 0 for m in self.config.metrics_list}

        print("\n" + "=" * 60)                      # Visual separator in logs
        print(f"[DB: {db_id}]")                     # Context: DB name
        print(f"TARGET QUERY:\n{query_gold}")       # Log gold query
        print(f"PREDICTION QUERY:\n{query_pred}")   # Log predicted query

        # -------- EM: exact string match after whitespace normalization --------
        if _norm_ws(query_gold) == _norm_ws(query_pred):  # Normalize and compare
            metrics['EM'] = 1
        print(f"Target query after normalization{_norm_ws(query_gold)}")
        print(f"Prediction query after normalization{_norm_ws(query_pred)}")
        print(f"Exact Match (EM): {bool(metrics['EM'])}") # Log EM result

        # -------- QSM: stage sequence equality --------
        try:
            stages1 = get_query_stages(query=query_gold)  # Extract stage sequence from gold
            stages2 = get_query_stages(query=query_pred)  # Extract stage sequence from pred
            print(f"TARGET stages: {stages1}")            # Log sequences
            print(f"PREDICT stages: {stages2}")
            metrics['QSM'] = int(stages1 == stages2)      # 1 if equal, else 0
        except Exception as e:
            print(f"QSM error for {db_id}: {e}")          # On extraction failure, set 0
            metrics['QSM'] = 0

        # -------- QFC: field coverage set equality --------
        try:
            fields1 = extract_fields(MQL=query_gold, db_name=db_id)  # Extract set/list of fields from gold
            fields2 = extract_fields(MQL=query_pred, db_name=db_id)  # Extract fields from pred
            print(f"TARGET fields: {fields1}")                        # Log both
            print(f"PREDICT fields: {fields2}")
            metrics['QFC'] = int(set(fields1) == set(fields2))        # Compare as sets; 1 if equal
        except Exception as e:
            print(f"QFC error for {db_id}: {e}")
            metrics['QFC'] = 0

        # -------- Execution-based metrics: EX, EFM, EVM --------
        try:
            result_gold = self._get_query_result(db_id, query_gold)   # Execute gold (via cache + fast path)
            result_pred = self._get_query_result(db_id, query_pred)   # Execute pred

            # Bounded previews (avoid expensive full dumps)
            print(f"TARGET result (preview):   {_preview_blob(result_gold, self.config.preview_chars)}")
            print(f"PREDICT result (preview):  {_preview_blob(result_pred, self.config.preview_chars)}")

            if self.config.log_exec_field_details:                    # Optional detailed field/value logging
                paths_gold, samples_gold = _collect_fields_and_values(
                    result_gold,
                    max_samples=self.config.value_samples_per_field,
                    max_chars=self.config.preview_chars // 6          # Keep per-value short
                )
                paths_pred, samples_pred = _collect_fields_and_values(
                    result_pred,
                    max_samples=self.config.value_samples_per_field,
                    max_chars=self.config.preview_chars // 6
                )

                print(f"TARGET field path count:  {len(paths_gold)}")  # Count of unique field paths in gold
                print(f"PREDICT field path count: {len(paths_pred)}")  # Count in pred

                missing_in_pred = sorted(paths_gold - paths_pred)      # Paths present in gold but not in pred
                extra_in_pred   = sorted(paths_pred - paths_gold)      # Paths present only in pred
                shared_paths    = sorted(paths_gold & paths_pred)      # Paths present in both

                # Cap how many to print to avoid log spam
                max_fields = self.config.max_logged_fields

                if missing_in_pred:
                    print(f"Missing in PREDICT ({min(len(missing_in_pred), max_fields)} shown):")
                    for pth in missing_in_pred[:max_fields]:
                        print(f"  - {pth}")

                if extra_in_pred:
                    print(f"Extra in PREDICT ({min(len(extra_in_pred), max_fields)} shown):")
                    for pth in extra_in_pred[:max_fields]:
                        print(f"  + {pth}")

                # Show sample values for a subset of shared fields
                if shared_paths:
                    print(f"Sample values for shared fields ({min(len(shared_paths), max_fields)} shown):")
                    for pth in shared_paths[:max_fields]:
                        g_vals = samples_gold.get(pth, [])             # Sample values from gold
                        p_vals = samples_pred.get(pth, [])             # Sample values from pred
                        print(f"  {pth}:")
                        if g_vals:
                            print(f"    TARGET samples : {g_vals}")
                        if p_vals:
                            print(f"    PREDICT samples: {p_vals}")
            
            # EX: deep equality of full result objects
            metrics['EX'] = int(self._deep_equal(result_gold, result_pred))  # 1 if exactly equal structure+values

            # EFM/EVM: per-doc field sets and value equality
            fields_gold, fields_pred = set(), set()   # Track field names encountered across docs
            metrics['EFM'] = 1                        # Assume match until proven otherwise
            metrics['EVM'] = 1

            def collect_fields(d: Any, acc: set):
                if isinstance(d, dict):
                    for k, v in d.items():
                        acc.add(k)                    # Add field key
                        collect_fields(v, acc)        # Recurse into value
                elif isinstance(d, (list, tuple)):
                    for it in d:
                        collect_fields(it, acc)       # Recurse into items

            for g, p in zip(result_gold, result_pred):  # Compare document-by-document (aligned by index)
                collect_fields(g, fields_gold)          # Collect all field names in gold doc
                collect_fields(p, fields_pred)          # Collect all field names in pred doc
                if not self._deep_equal(g, p):          # If any doc pair differs deeply
                    metrics['EVM'] = 0                  # Mark value mismatch

            if fields_gold != fields_pred:              # If the union of fields differ across sides
                metrics['EFM'] = 0                      # Mark field mismatch

        except Exception as e:
            print(f"Execution error for {db_id}: {e}")   # Any exec failure → mark EX/EFM/EVM = 0
            metrics['EX'] = metrics['EFM'] = metrics['EVM'] = 0

        print(f"Final metrics: {metrics}")               # Log final metric flags for this example
        print("=" * 60 + "\n")                           # Separator
        return metrics                                   # Return metric dict to aggregator


# Wrap heavy helpers with timers so you can see breakdowns
get_query_stages = timed("qsm")(get_query_stages)        # Wrap stage-extractor to time its total cost
extract_fields = timed("qfc")(extract_fields)            # Wrap field-extractor to time its total cost


# ----------------------------
# Aggregator
# ----------------------------

class AccuracyCalculator:
    """Aggregates metrics across examples and optionally logs wrong cases."""

    def __init__(self, config: MetricConfig):
        self.config = config                             # Save config
        self.comparator = QueryComparator(config)        # Build a comparator once

    def _format_example(self, example: Dict, acc: Dict) -> Dict:
        """Minimal info for wrong-case logging."""
        return {
            "NLQ": example['NLQ'],
            "db_id": example['db_id'],
            "prediction": example['prediction'],
            "target": example['target'],
            "flag": acc['EX'] == 1                      # True if execution matched
        }

    def _format_metrics_string(self, metrics: Dict[str, float]) -> str:
        return f"""
    Exact Match (EM): {metrics['EM']}
    Query Stages Match (QSM): {metrics['QSM']}
    Query Fields Coverage (QFC): {metrics['QFC']}
    Execution Accuracy (EX): {metrics['EX']}
    Execution Fields Match (EFM): {metrics['EFM']}
    Execution Value Match (EVM): {metrics['EVM']}
"""                                                      # Nicely formatted block for overall metrics

    def _save_wrong_examples(self, wrong_examples: List[Dict]):
        self.config.wrong_examples_path.parent.mkdir(parents=True, exist_ok=True)  # Ensure directory exists
        with open(self.config.wrong_examples_path, "w", encoding="utf-8") as f:   # Write JSON of failures
            json.dump(wrong_examples, f, indent=2, ensure_ascii=False)

    def calculate(
        self,
        examples: List[Dict],
        need_print: bool = False,
        need_save: bool = False
    ) -> Tuple[Dict[str, float], str]:

        metrics_sum = {metric: 0 for metric in self.config.metrics_list}  # Running totals for each metric
        wrong_examples = []                               # Collect wrong cases for optional saving
        total = len(examples)                             # N examples for averaging

        with timer("Processing examples"):                # Time the entire evaluation loop
            for ex in tqdm(examples, desc="Processing examples"):  # Progress bar over examples
                try:
                    ex_metrics = self.comparator.compare( # Compute metrics for this example
                        ex['target'],                     # gold MQL
                        ex['prediction'],                 # predicted MQL
                        ex['db_id'],                      # database name
                    )
                    # Accumulate 0/1 flags
                    for k, v in ex_metrics.items():
                        metrics_sum[k] += 1 if v else 0   # Sum up as integers (0 or 1)

                    # Log wrong examples based on EX
                    if ex_metrics.get('EX', 1) == 0:      # If execution didn't match
                        wrong_examples.append(self._format_example(ex, ex_metrics))

                except Exception as e:
                    print(f"\nExample error on db_id={ex.get('db_id')}: {e}\n")  # Robust per-example error handling

        # Compute means
        metrics_mean = {
            k: (metrics_sum[k] / total if total else 0.0)  # Average per metric
            for k in self.config.metrics_list
        }

        acc_str = self._format_metrics_string(metrics_mean)  # Prettified summary

        # Print final metrics + timing
        if need_print:
            print(acc_str)                                   # Print summary metrics
            if wrong_examples:
                print(f"\nTotal errors: {len(wrong_examples)} out of {total} examples")  # Show # wrong cases

            if _TIMINGS:                                     # If we recorded timing buckets
                print("\nTiming breakdown (seconds):")
                for k, v in sorted(_TIMINGS.items()):
                    print(f"  {k:>6}: {v:.3f}")              # Print per-label totals (e.g., exec, qsm, qfc)

        if need_save:
            self._save_wrong_examples(wrong_examples)        # Optionally save wrong-case JSON

        return metrics_mean, acc_str                         # Return (dict, string) to caller


# ----------------------------
# CLI entry (same shape as yours)
# ----------------------------

# Example of running:
# python ./src/utils/metrics.py
if __name__ == "__main__":                                   # Only run when executed as a script (not imported)
    # Choose which dataset to score (adjust to your paths)
    file_name = "results3"                                     # Base name for input/output paths
    predictions_path = f"../results/{file_name}.json"        # Input results file (formatted JSON rows)

    # Set up logging (optional)
    log_path = f"./logs/{file_name}_metrics.log"             # Where to save the full console output
    Path("./logs").mkdir(exist_ok=True)                      # Ensure logs directory exists

    with open(log_path, "w", encoding="utf-8") as log_file:  # Open log file for writing
        with redirect_stdout(log_file), redirect_stderr(log_file):  # Redirect both stdout/stderr to file
            print(f"File name: {file_name}")                 # First line in the log

            config = MetricConfig(                           # Build config with desired knobs
                cache_size=10000,                            # (Informational; lru_cache size set at decorator)
                wrong_examples_path=Path(f'./error_case/{file_name}.json'),
                preview_chars=1500,                          # Limit preview size
                allow_disk_use=True,                         # Allow large aggregations
            )

            calculator = AccuracyCalculator(config)          # Create calculator instance

            # predictions file is your formatted_results.json-like array
            with open(predictions_path, 'r', encoding='utf-8') as f:  # Read the predictions dataset
                predictions = json.load(f)

            # Re-map to the evaluator's expected structure
            # Each example requires: db_id, NLQ, target (gold MQL), prediction (pred MQL)
            results = [{                                       # Build list of evaluation examples
                "db_id": ex['db_id'],                          # Database identifier
                "NLQ": ex.get('nlq', ''),                      # Natural-language query (optional)
                "target": ex['MQL'],                           # Gold Mongo pipeline (string)
                "prediction": ex['MQL_pred'],                  # Predicted Mongo pipeline (string)
            } for ex in predictions]

            # Run metrics (and print summary to the log file)
            metric, metric_str = calculator.calculate(results, need_print=True)  # Compute metrics; print to log

    print(f"Log saved to {log_path}")                         # Print log location to console (stdout)
