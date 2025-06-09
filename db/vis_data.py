from vector_store import store_analysis, vectorstore, retrieve_similar_logs

query = ""
docs = retrieve_similar_logs(query)
for doc in docs:
    print(f"Content: {doc.page_content}")
    print(f"Metadata: {doc.metadata}")
    print("-" * 40)