import pandas as pd
from db.vector_store import store_analysis, vectorstore, retrieve_similar_logs

# Your query
query = ""
docs = retrieve_similar_logs(query)

# Create a list to store all document data
data = []

for doc in docs:
    # Create a dictionary for each document
    row = {
        "content": doc.page_content,
        "dag_run_date": doc.metadata.get("dag_run_date", ""),
        "dag_run_id": doc.metadata.get("dag_run_id", ""),
        "proposed_solution": doc.metadata.get("proposed_solution", ""),
        "actions": doc.metadata.get("actions", ""),
        "analysis_id": doc.metadata.get("analysis_id", ""),
        "dag_id": doc.metadata.get("dag_id", ""),
        "composite_id": doc.metadata.get("composite_id", ""),
    }
    data.append(row)

# Create DataFrame
df = pd.DataFrame(data)

# Export to Excel
excel_filename = "vector_db_results.xlsx"
df.to_excel(excel_filename, index=False, engine="openpyxl")

print(f"Data exported to {excel_filename}")
print(f"Total records exported: {len(data)}")
