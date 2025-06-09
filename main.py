from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from dotenv import load_dotenv
from slack_handler.events import slack_events
import logging
from agent.graph import graph
from pydantic import BaseModel
from agent.model import model

# Load environment variables from .env file
load_dotenv()

# Create FastAPI app instance
app = FastAPI()

# Configure logging (optional, but good practice)
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Include the Slack events router
app.include_router(slack_events, prefix="/slack")


# Define the request model for the chat query
class QueryRequest(BaseModel):
    query: str


@app.post("/chat")
async def query_agent(request: QueryRequest):
    try:
        user_message = request.query

        response = model.invoke(user_message)

        return {"response": response.content}

    except Exception as e:
        logger.exception("Error handling chat request:")
        raise HTTPException(status_code=500, detail=str(e))
