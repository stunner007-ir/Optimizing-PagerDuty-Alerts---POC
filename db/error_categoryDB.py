import os
import re
import pandas as pd
from dotenv import load_dotenv
from langchain_openai import OpenAIEmbeddings
from langchain_community.document_loaders import UnstructuredExcelLoader
from langchain_text_splitters import RecursiveCharacterTextSplitter
from langchain_chroma import Chroma
from langchain.schema import Document

# Load environment variables
load_dotenv()

# Constants
CHROMA_DIR = "chroma_db"
EXCEL_FILE_PATH = "db/Error_Category.xlsx"

# Load API credentials
base_url = os.getenv("RAKUTEN_AI_EMBEDDING_URL")
api_key = os.getenv("RAKUTEN_AI_API_KEY")

# Create directory if not exists
os.makedirs(CHROMA_DIR, exist_ok=True)
# Create embedding model with timeout
embeddings = OpenAIEmbeddings(
    model="text-embedding-3-small",
    api_key=api_key,
    base_url=base_url,
    request_timeout=60,
)

# Read and clean Excel data
df = pd.read_excel(EXCEL_FILE_PATH)
df = df.dropna(how="all")  # Remove fully empty rows


# Clean text from each row
def clean_text(text):
    text = text.replace("\xa0", " ")  # Remove non-breaking space
    text = re.sub(r"\s+", " ", text)  # Collapse multiple spaces/newlines
    return text.replace(" | nan", " | Unknown").strip()


# Convert rows to clean text format
def row_to_text(row):
    try:
        return clean_text(
            " | ".join(
                str(row[col]) if pd.notna(row[col]) else "" for col in df.columns
            )
        )
    except Exception as e:
        print(f"⚠️ Row processing error: {e}")
        return ""


texts = [row_to_text(row) for _, row in df.iterrows()]
texts = [text for text in texts if text]  # Filter out blank ones

# Create documents
documents = [
    Document(page_content=text, metadata={"row_index": i})
    for i, text in enumerate(texts)
]

# Split documents into smaller chunks
text_splitter = RecursiveCharacterTextSplitter(
    chunk_size=500, chunk_overlap=50, length_function=len
)
documents = text_splitter.split_documents(documents)

# Create or load Chroma vector store
if os.path.exists(os.path.join(CHROMA_DIR, "chroma.sqlite3")):
    print("✅ Loading existing Chroma database...")
    vectordb = Chroma(
        persist_directory=CHROMA_DIR,
        embedding_function=embeddings,
        collection_name="error_categories",
    )
    print("✅ Vector store loaded")

else:
    print("✅ Creating new Chroma database...")
    vectordb = Chroma.from_documents(
        documents,
        embeddings,
        persist_directory=CHROMA_DIR,
        collection_name="error_categories",
    )
    print("✅ Vector store initialized")


# Function to store new error entries
def store_error_categories(
    error_message: str, error_category: str, error_type: str, actions: str
):
    text = f"Error: {error_message}. Category: {error_category}. Type: {error_type}. Actions: {actions}"
    metadata = {
        "error_message": error_message,
        "error_category": error_category,
        "error_type": error_type,
        "actions": actions,
    }
    text = clean_text(text)
    vectordb.add_texts(texts=[text], metadatas=[metadata])
    vectordb.persist()
    print("📝 New error entry stored.")


# Function to retrieve similar error/action entries
def retrieve_similar_error_action(query: str, k: int = 3):
    query = clean_text(query)
    results = vectordb.similarity_search(query, k=k)
    print(f"🔍 Retrieved {len(results)} similar results for query: '{query}'")
    return results
