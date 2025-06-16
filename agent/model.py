from langchain_openai.chat_models import ChatOpenAI
from langchain_openai import OpenAIEmbeddings
import os
from dotenv import load_dotenv

load_dotenv()

base_url = os.getenv("RAKUTEN_AI_URL")
api_key = os.getenv("RAKUTEN_AI_API_KEY")
embedding_base_url = os.getenv("RAKUTEN_AI_EMBEDDING_URL")


if not api_key:
    raise ValueError(
        "API key not found. Set the RAKUTEN_AI_API_KEY environment variable."
    )

if not base_url:
    raise ValueError("Base URL not found. Set the RAKUTEN_AI_URL environment variable.")


# Initialize the ChatOpenAI model
try:
    model = ChatOpenAI(
        model="o4-mini",  # Double-check this model name is correct for Rakuten AI
        max_retries=2,
        base_url=base_url,
        api_key=api_key,
    )
except Exception as e:
    print(f"Error initializing ChatOpenAI: {e}")
    model = None  

try:
    embedding_model = OpenAIEmbeddings(
        model="text-embedding-3-large",  
        api_key=api_key,
        base_url=embedding_base_url,
        request_timeout=60,

    )
except Exception as e:
    print(f"Error initializing OpenAIEmbeddings: {e}")
    embedding_model = None 
