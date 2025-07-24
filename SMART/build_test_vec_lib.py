import openai
import numpy as np
from scipy.spatial.distance import cosine
import json
import pandas as pd
from tqdm import tqdm
import pickle
import time
from dotenv import load_dotenv
import os
load_dotenv()

# Builds a vector library for test data using OpenAI embeddings, 
data_path = "../TEND/test_SLM_subset.json"
vql_embedding_file_path = "./vector_store/test_subset.pkl"
# Originally
# path = "./TEND/test_SLM_prediction.json"
# Changed to use a smaller subset for testing

api_base = "https://api.openai.com/v1"

# replace with your OpenAI API key
api_key = os.getenv("OPENAI_API_KEY")

client = openai.Client(api_key=api_key, base_url=api_base)
cache = {}

# Thinking of changing embedding model to text-embedding-3-small, newer and faster, but not sure if it will work with the current code
def get_embedding(text, model="text-embedding-ada-002"):
    if text in cache:
        return cache[text]
    embedding = None
    while embedding is None:
        try:
            embedding = client.embeddings.create(input=text, model=model)
        except Exception as ex:
            print(ex)
            
    cache[text] = np.array(embedding.data[0].embedding)
    return np.array(embedding.data[0].embedding)


if __name__ == '__main__':
    
    data_new = []
    # if os.path.exists(vql_embedding_file_path):
    #     with open(vql_embedding_file_path, 'rb') as f: 
    #         data_new = pickle.load(f)
    with open(data_path, 'r', encoding='utf-8') as f:
        data = json.load(f)


    for index, example in tqdm(enumerate(data), total=len(data)):
        if index < len(data_new):
            continue
        
        nlq = example['nlq']
        MQL = example['MQL']
        query_collection = example['query_collection'].split(", ")
        fields_db = example['fields_db'].split(", ")
        fields_alias = example['fields_alias'].split(", ")
        target_fields = example['target_fields'].split(", ")
        
        query_collection.sort()
        fields_db.sort()
        fields_alias.sort()
        target_fields.sort()

        query_collection = ", ".join(query_collection)
        fields_db = ", ".join(fields_db)
        fields_alias = ", ".join(fields_alias)
        target_fields = ", ".join(target_fields)

        nlq_emb = get_embedding(nlq)
        MQL_emb = get_embedding(MQL)
        query_collection_emb = get_embedding(query_collection)
        fields_db_emb = get_embedding(fields_db)
        fields_alias_emb = get_embedding(fields_alias)
        target_fields_emb = get_embedding(target_fields)

        
        nlq_emb = get_embedding(example['nlq'])
        
        example_new = {
            "nlq":{"value":nlq, "embedding":nlq_emb},
            "db_id":example['db_id'],
            "mql":{"value":MQL, "embedding":MQL_emb},
            "fields_db":{"value":fields_db, "embedding":fields_db_emb},
            "fields_alias":{"value":fields_alias, "embedding":fields_alias_emb},
            "target_fields":{"value":target_fields, "embedding":target_fields_emb},
            "query_collection":{"value":query_collection, "embedding":query_collection_emb}
        }

        data_new.append(example_new)

        if index % 10 == 0:
            with open(vql_embedding_file_path, 'wb') as f:
                pickle.dump(data_new, f)

    os.makedirs(os.path.dirname(vql_embedding_file_path), exist_ok=True)

    with open(vql_embedding_file_path, 'wb') as f:
        pickle.dump(data_new, f)