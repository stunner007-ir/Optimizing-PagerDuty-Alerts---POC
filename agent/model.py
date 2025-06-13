from langchain_openai.chat_models import ChatOpenAI
import os
from dotenv import load_dotenv
from langchain_openai import OpenAIEmbeddings


load_dotenv()

base_url = os.getenv("RAKUTEN_AI_URL")
api_key = os.getenv("RAKUTEN_AI_API_KEY")


if not api_key:
    raise ValueError(
        "API key not found. Set the RAKUTEN_AI_API_KEY environment variable."
    )

# Initialize the ChatOpenAI model
model = ChatOpenAI(
    model="o4-mini",
    max_retries=2,
    # temperature=0.4,
    base_url=base_url,
    api_key=api_key,
)

embedding_model = OpenAIEmbeddings(
    model="text-embedding-3-large", api_key=api_key, base_url=base_url
)
