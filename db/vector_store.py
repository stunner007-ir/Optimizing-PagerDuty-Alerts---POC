import os
# from langchain_community.vectorstores import Chroma
from langchain_chroma import Chroma
from agent.model import embedding_model

CHROMA_DIR = "chroma_db"
os.makedirs(CHROMA_DIR, exist_ok=True)

vectorstore = Chroma(
    persist_directory=CHROMA_DIR,
    embedding_function=embedding_model,
    collection_name="dag_analyses",
)


def store_analysis(
    dag_id: str,
    dag_run_id: str,
    dag_run_date: str,
    logs_analysis: str,
    proposed_solution: str,
    analysis_id: str,
    actions: str = "rerun",
):
    composite_id = f"{dag_id}::{dag_run_id}::{dag_run_date}"
    metadata = {
        "composite_id": composite_id,
        "analysis_id": analysis_id,
        "proposed_solution": proposed_solution,
        "actions": actions,
        "dag_id": dag_id,  # keep for reference
        "dag_run_id": dag_run_id,
        "dag_run_date": dag_run_date,
    }

    text = f"Logs and Analysis:\n{logs_analysis}\n\nProposed Solution:\n{proposed_solution}"
    vectorstore.add_texts([text], metadatas=[metadata])
    vectorstore.persist()


def retrieve_similar_logs(query: str, k: int = 3):
    # Returns a list of Document objects with .page_content and .metadata fields
    return vectorstore.similarity_search(query, k=k)
