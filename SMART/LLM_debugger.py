'''
LLM Generator is responsible for recieving the information integrated by Integrator and generating MQL
'''

import json
from tqdm import tqdm
import os
import argparse
from utils.utils import generate_reply
from utils.schema_to_markdown import schemas_transform


NEED_PRINT = False

SYSTEM_PROMPT = """You are now the query fine-tuner in the MongoDB natural language interface, responsible for ensuring that the final MongoDB query meets the user's expectations.
You need to first analyze whether the original MongoDB query needs adjustment based on the natural language query and the MongoDB collections and their fields. (i) If no adjustment is needed, retain the original MongoDB query; (ii) If adjustment is needed, make step-by-step adjustments to the original MongoDB query according to the guidelines."""

INSTRUCTION = """#### Given MongoDB collections and their fields, a natural language query, query transformation reference examples, and the original MongoDB query, please perform the following actions:
1. Analyze whether the original MongoDB query needs adjustment based on the natural language query and the MongoDB collections and their fields:
   - If adjustments are needed, analyze the natural language query based on the MongoDB collections and their fields (only adjust if necessary);
   - If no adjustments are needed, retain the original MongoDB query and proceed directly to step three for output;
2. Adjust the original MongoDB query based on the query transformation reference examples and the analysis from step one, then proceed to step three for output;
3. Output the final MongoDB query in the following format:
```javascript
db.collection.aggregate([pipeline]); / db.collection.find({[filter]}, {[projection]});
```"""

def prompt_maker(NLQ, mql_ori, db_id, cols, fields_db, fields_alias, target_fields, rag_examples):
    cols_list = cols.split(", ")
    cols_list.sort()
    scehmas_str = schemas_transform(db_id=db_id)

    rag_dict = {}
    for rag_example in rag_examples:
        NLQ_rag = rag_example['NLQ']
        fields_db_rag = rag_example['fields_db']
        fields_alias_rag = rag_example['fields_alias']
        target_fields_rag = rag_example['target_fields']
        query_collection_rag = rag_example['query_collection']
        
        MQL_rag = rag_example['MQL'].strip("\n").strip()
        try:
            rag_dict[MQL_rag]['NLQ'].append(NLQ_rag)
        except:
            rag_dict[MQL_rag] = {}
            rag_dict[MQL_rag]['NLQ'] = [NLQ_rag]
            rag_dict[MQL_rag]['fields_db'] = fields_db_rag
            rag_dict[MQL_rag]['fields_alias'] = fields_alias_rag
            rag_dict[MQL_rag]['target_fields'] = target_fields_rag
            rag_dict[MQL_rag]['query_collection'] = query_collection_rag

# This is the part that formats the rag examples into a string
    rag_str = ""
    for id, (rag_mql, rag_example) in enumerate(rag_dict.items()):
        nlqs_str = ""
        for nlq in rag_example['NLQ']:
            nlqs_str += f"   - `{nlq}`\n"
        nlqs_str = nlqs_str.strip("\n").strip()
        #This below code is the actual formatting of each few shot example
        rag_str += f"""{id+1}. Example {id+1}
# Natural Language Query
{nlqs_str}
# MongoDB Collections Used in MongoDB Query
   - {rag_example['query_collection']}
# MongoDB Fields Used in MongoDB Query
   - {rag_example['fields_db']}
# Renamed Fields Used in MongoDB Query
   - {rag_example['fields_alias']}
# Fields shown in Execution Document
   - {rag_example['target_fields']}
# MongoDB Query
```javascript
{rag_mql}
```

"""

    rag_str = rag_str.strip("\n").strip()
    instruction = INSTRUCTION.strip("\n").strip()
    prompt = f"""### Query Transformation Reference Examples
{rag_str}

###  MongoDB collections and their fields
{scehmas_str}

### Natural Language Query
   - `{NLQ}`

### Original MongoDB Query
```javascript
{mql_ori}
```

### MongoDB Collections may be Used in MongoDB Query
   - {cols}
### MongoDB Fields may be Used in MongoDB Query
   - {fields_db}
### Renamed Fields may be Used in MongoDB Query
   - {fields_alias}
### Fields may be shown in Execution Document
   - {target_fields}

{instruction}

A: Let's think step by step! """

    if NEED_PRINT:
        print(prompt, end="\n" + "*"*100 + "\n")

    return prompt

def query_debug(NLQ, mql_ori, db_id, cols, fields_db, fields_alias, target_fields, rag_examples):
    prompt = prompt_maker(NLQ, mql_ori.strip("\n").strip(), db_id, cols, fields_db, fields_alias, target_fields, rag_examples)
    messages = [
        {
            "role":"system",
            "content":SYSTEM_PROMPT
        },
        {
            "role":"user",
            "content":prompt
        }
    ]

    with open("./prompt.txt", "w") as f:
        f.write(prompt)
    # exit()
    # reply = generate_reply(messages=messages, model="gpt-4o-mini-2024-07-18")[0]

    if index == 0:
        print("\n" + "="*30 + " PROMPT SENT TO GPT " + "="*30)
        for m in messages:
            print(f"[{m['role'].upper()}]: {m['content']}\n")

    reply = generate_reply(messages=messages)[0]

    if index == 0:
        print("="*30 + " GPT OUTPUT " + "="*30)
        print(reply)
        print("="*70 + "\n")

    # reply = generate_reply(messages=messages)[0]

    if NEED_PRINT:
        print(reply, end= "\n" + "*"*100 + "\n")

    with open("./prompt.txt", "a") as f:
        f.write("\n\n" + reply)
        # Was originally f.write("\n\n" + f)
    
    # exit()
    if "```javascript" in reply:
        reply = reply.rsplit("```javascript", 1)[-1].rsplit("```", 1)[0]
        
    rows_new = []
    for row in reply.split("\n"):
        if "//" in row:
            row = row.split("//", 1)[0]
        rows_new.append(row)
    reply = "\n".join(rows_new)
    reply = reply.strip("\n").strip()
    return reply


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="LLM Retune Args.")
    parser.add_argument("--topk", default=2, type=int, help="Num of Retrieval Example")
    # Originally, since top 20 similar examples were used
    # parser.add_argument("--topk", default=20, type=int, help="Num of Retrieval Example")
    args = parser.parse_known_args()[0]
    topk = args.topk
    file_path = "../TEND/test_SLM_subset_rag_no_pref.json"
    #file_path = "./results/SMART/test_SLM_prediction_rag.json"
    save_path = "./results/test_debug_rag{}.json".format(topk)
    os.makedirs(os.path.dirname(save_path), exist_ok=True)

    with open(file_path, "r") as f:
        test_data = json.load(f)

    test_data_new = []
    if os.path.exists(save_path):
        with open(save_path, "r") as f:
            test_data_new = json.load(f)
    

    for index, example in tqdm(enumerate(test_data), total=len(test_data)):
        if index < len(test_data_new):
            continue
        NLQ = example['nlq']
        db_id = example['db_id']
        rag_examples = example['RAG_examples'][:topk]
        #rag_examples.reverse()
        mql_ori = example["MQL"]
        cols = example["query_collection"]
        fields_db = example["fields_db"]
        fields_alias = example["fields_alias"]
        target_fields = example["target_fields"]
        # This extracts fields from the examples
        

        prediction = query_debug(NLQ, mql_ori, db_id, cols, fields_db, fields_alias, target_fields, rag_examples)
        # Now inside query_debug it constructs the prompt, calls schema_transform

        example_new = example.copy()
        example_new['MQL_debug'] = prediction

        test_data_new.append(example_new)

        if index % 20 == 0:
            with open(save_path, "w") as f:
                json.dump(test_data_new, f, indent=4)
    with open(save_path, "w") as f:
        json.dump(test_data_new, f, indent=4)



