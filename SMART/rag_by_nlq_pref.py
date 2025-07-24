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

# This is add RAG examples to each test case and save it to test_SLM_subset_rag_no_pref.json, after which the LLM_Optimizer.py will be used to optimize the MQL queries based on the RAG examples.
# For each test question it finds similar training examples using cosine similarity, top-k similar training examples are picked as RAG examples and attached to the test question, this is all saved to test_SLM_subset_rag_no_pref.json

# train_emb_path = "./vector_store/train.pkl"
# test_emb_path = "./vector_store/test.pkl"
train_emb_path = "./vector_store/train_subset.pkl"
test_emb_path = "./vector_store/test_subset.pkl"

# data_path = "./TEND/test_SLM_prediction.json"
# result_save_path = "./TEND/test_SLM_prediction_rag_no_pref.json"
data_path = "../TEND/test_SLM_subset.json"
result_save_path = "../TEND/test_SLM_subset_rag_no_pref.json"

api_base = "https://api.openai.com/v1"

# replace with your OpenAI API key
api_key = os.getenv("OPENAI_API_KEY")

client = openai.Client(api_key=api_key, base_url=api_base)

cache = {}

with open(train_emb_path, 'rb') as f:
    data_embedding_all = pickle.load(f)

data_embedding_all_pd = pd.DataFrame(data_embedding_all)
data_embeddings = data_embedding_all_pd.to_dict(orient='records')

def get_embedding(text, model="text-embedding-ada-002"):
    if text in cache:
        return cache[text]
    embedding = client.embeddings.create(input=text, model=model)
    cache[text] = np.array(embedding.data[0].embedding)
    return np.array(embedding.data[0].embedding)


def rag_by_nlq_pref(nlq_emb, rough_mql_emb, fields_db_emb, fields_alias_emb, target_fields_emb, collection_emb, k=1) -> list:

    # calculate the similarity between the question vector and each sentence vector in the document
    similarities = []
    for example_emb in data_embeddings:
        nlq_sim = 1 - cosine(example_emb['nlq']['embedding'], nlq_emb)
        mql_sim = 1 - cosine(example_emb['mql']['embedding'], rough_mql_emb)
        fields_db_sim = 1 - cosine(example_emb['fields_db']['embedding'], fields_db_emb)
        fields_alias_sim = 1 - cosine(example_emb['fields_alias']['embedding'], fields_alias_emb)
        target_fields_sim = 1 - cosine(example_emb['target_fields']['embedding'], target_fields_emb)
        collection_sim = 1 - cosine(example_emb['query_collection']['embedding'], collection_emb)

        simi = nlq_sim*1 + mql_sim*0.3 + fields_db_sim*0.7 + fields_alias_sim*0.5 + target_fields_sim*0.5 + collection_sim*0.7
        
        similarities.append(simi)

    # Select top-k most similar sentences
    top_k_indices = np.argsort(similarities)[-k:][::-1]
    top_k_row = data_embedding_all_pd.loc[top_k_indices.tolist()][['nlq', 'mql', 'fields_db', 'fields_alias', 'target_fields', 'db_id', 'query_collection']]

    examples = []
    for index, row in top_k_row.iterrows():
            example = {
                "db_id":row['db_id'],
                "NLQ":row['nlq']['value'],
                "MQL":row['mql']['value'],
                "fields_db":row['fields_db']['value'],
                "fields_alias":row['fields_alias']['value'],
                "target_fields":row['target_fields']['value'],
                "query_collection":row['query_collection']['value']
            }
            examples.append(example)

    return examples

def rag_by_nlq(nlq_emb, k=1) -> list:

    # Calculate the similarity between the question vector and each sentence vector in the document
    similarities = []
    for example_emb in data_embeddings:
        nlq_sim = 1 - cosine(example_emb['nlq']['embedding'], nlq_emb)

        simi = nlq_sim*1
        
        similarities.append(simi)

    # Select top-k most similar sentences
    top_k_indices = np.argsort(similarities)[-k:][::-1]
    top_k_row = data_embedding_all_pd.loc[top_k_indices.tolist()][['nlq', 'mql', 'db_id', 'fields_db', 'fields_alias', 'target_fields', 'query_collection']]

    examples = []
    for index, row in top_k_row.iterrows():
            example = {
                "db_id":row['db_id'],
                "NLQ":row['nlq']['value'],
                "MQL":row['mql']['value'],
                "fields_db":row['fields_db']['value'],
                "fields_alias":row['fields_alias']['value'],
                "target_fields":row['target_fields']['value'],
                "query_collection":row['query_collection']['value']
            }
            examples.append(example)

    return examples

if __name__ == '__main__':
    data_new = []
    if os.path.exists(result_save_path):
        with open(result_save_path, 'r', encoding='utf-8') as f: 
            data_new = json.load(f)
    with open(test_emb_path, 'rb') as f:
        data = pickle.load(f)

    with open(data_path, "r", encoding='utf-8') as f:
        test_data = json.load(f)
    
    for index, (example, example_test) in tqdm(enumerate(zip(data, test_data)), total=len(data)):
        if index < len(data_new):
            continue

        nlq_emb = example['nlq']['embedding']
        
        # rag_examples = rag_by_nlq(nlq_emb, k=20) 
        # Originally 20, finds top 20 most similar training examples
        rag_examples = rag_by_nlq(nlq_emb, k=2)
        example_new = example_test.copy()

        example_new['RAG_examples'] = rag_examples

        data_new.append(example_new)

        if index % 20 == 0:
            with open(result_save_path, 'w') as f:
                json.dump(data_new, f, indent=4)

    with open(result_save_path, 'w') as f:
        json.dump(data_new, f, indent=4)
        