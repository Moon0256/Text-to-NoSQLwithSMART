# echo "build retrieval codebase..."
# python ./build_vec_lib.py

# echo "retrieve code examples..."
# python ./rag_by_nlq_pref.py 

# # After getting the SLM prediction, run the following command
# echo "get SLM predictions..."
# python ./get_SLM_prediction.py

topk=${1:-20}

echo "LLM Debug..."
python ./SMART/LLM_debugger.py --topk $topk

echo "LLM Optimize..."
python ./SMART/LLM_Optimizer.py --topk $topk