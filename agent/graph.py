from typing import TypedDict, List, Dict, Optional
import time
from langgraph.graph import StateGraph
from langchain_core.messages import BaseMessage, HumanMessage
from agent.tools.tools import (
    tools,
    fetch_log_details,
    send_to_slack,
    rerun_dag,
)
from db.vector_store import store_analysis, vectorstore
import logging
import uuid
import json
from agent.model import model
from dotenv import load_dotenv

# Logging Setup
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

load_dotenv()


# State Definition
class GraphState(TypedDict):
    messages: List[BaseMessage]
    dag_name: Optional[str]
    dag_run_date: Optional[str]
    dag_run_id: Optional[str]
    logs: Optional[str]
    analysis_results: Optional[str]
    solution: Optional[str]
    dag_status: Optional[str]
    user_message: Optional[str]
    llm_response: Optional[str]
    analysis_id: Optional[str]
    slack_sent: Optional[bool]  # Track if Slack message has been sent
    rerun_attempted: Optional[bool]  # Track if rerun has been attempted


# Model
model_with_tools = model.bind_tools(tools)


# Node Functions
def extract_dag_info(state: GraphState) -> Dict[str, Optional[str]]:
    node_name = "extract_dag_info"

    text_details = state.get("messages").get(
        "text_details"
    )  # Assuming messages is a list
    if isinstance(text_details, str):
        try:
            text_details = json.loads(text_details)
        except json.JSONDecodeError:
            text_details = {}

    result = {
        "dag_name": text_details.get("dag_name"),
        "dag_run_id": text_details.get("run_id"),
        "dag_run_date": text_details.get("run_date"),
        "dag_status": text_details.get("dag_status"),
        "user_message": text_details.get("full_text"),
        "slack_sent": False,  # Initialize slack_sent to False
        "rerun_attempted": False,  # Initialize rerun_attempted to False
    }
    return result


def route_logs(state: GraphState) -> str:
    node_name = "route_logs"
    start_time = time.time()

    dag_status = state.get("dag_status")

    if dag_status == "failed":
        if state.get("dag_name") and state.get("dag_run_id"):
            return "get_logs"
    return "do_not_fetch_logs"


def get_logs(state: GraphState) -> Dict[str, str]:
    node_name = "get_logs"
    start_time = time.time()

    dag_name = state.get("dag_name")

    try:
        logs = fetch_log_details(dag_name=dag_name)
        
        return {"logs": str(logs)}
    except Exception as e:
        return {"logs": f"Error: {e}"}


def do_not_fetch_logs(state: GraphState) -> Dict[str, str]:
    return {"logs": "No logs to fetch."}


def check_for_duplicate_analysis(state: GraphState) -> Optional[Dict[str, str]]:
    node_name = "check_for_duplicate_analysis"
    start_time = time.time()

    dag_id = state.get("dag_name")
    dag_run_id = state.get("dag_run_id")
    dag_run_date = state.get("dag_run_date")

    if not (dag_id and dag_run_id and dag_run_date):
        return None

    composite_id = f"{dag_id}::{dag_run_id}::{dag_run_date}"
    filter_metadata = {"composite_id": composite_id}

    try:
        similar_docs = vectorstore.similarity_search(
            query="", k=1, filter=filter_metadata
        )
        if similar_docs:
            doc = similar_docs[0]
            return {
                "analysis_results": doc.page_content,
                "analysis_id": doc.metadata.get("analysis_id", "unknown"),
            }
        return None
    except Exception:
        return None


def analyze_logs(state: GraphState) -> Dict[str, str]:
    node_name = "analyze_logs"
    start_time = time.time()

    logs = state.get("logs", "")
    dag_id = state.get("dag_name", "")
    dag_run_id = state.get("dag_run_id", "")
    dag_run_date = state.get("dag_run_date", "")

    # Check for duplicate analysis
    cached = check_for_duplicate_analysis(state)
    if cached:
        return cached

    try:
        prompt = (
            f"Please analyze the following logs from DAG '{dag_id}':\n\n{logs}\n\n"
            "Respond in the following format:\n"
            "Analysis: <your analysis of the issue>\n"
            "Solution: <a clear, actionable solution to resolve the issue>"
        )

        response = model.invoke([HumanMessage(content=prompt)]).content
        solution_text = ""

        if "Solution:" in response:
            parts = response.split("Solution:")
            solution_text = parts[1].strip()  # Only keep the solution part

        analysis_id = str(uuid.uuid4())

        # Store only the solution in the analysis
        store_analysis(
            dag_id=dag_id,
            dag_run_id=dag_run_id,
            dag_run_date=dag_run_date,
            logs_analysis="",  # No analysis text to store
            proposed_solution=solution_text,
            analysis_id=analysis_id,
            actions="rerun",  # empty initially
        )

        return {
            "analysis_results": "",
            "analysis_id": analysis_id,
            "proposed_solution": solution_text,
        }  # Return empty analysis_results

    except Exception as e:
        return {"analysis_results": f"Analysis failed: {e}"}
    

def bucket_error_analysis(state: GraphState) -> Dict[str, str]:
    """
    Store error analysis and solutions as buckets in vectorstore
    to facilitate retrieval for similar future errors.
    """
    node_name = "bucket_error_analysis"
    logs = state.get("logs", "")
    dag_id = state.get("dag_name", "")
    analysis_id = state.get("analysis_id", "")
    proposed_solution = state.get("proposed_solution", "")
    rerun_attempted = state.get("rerun_attempted", False)
    
    if not (logs and dag_id and analysis_id and proposed_solution):
        return {"bucket_response": "Insufficient data to create bucket."}
    
    try:
        # Extract a concise error snippet from logs (you can improve this logic)
        error_snippet = logs[:500]  # just take first 500 chars for indexing
        
        # Possible actions - could be extracted from state or default
        actions = "rerun" if not rerun_attempted else "manual_intervention"
        
        # Create a composite ID or use analysis_id as bucket ID
        bucket_id = analysis_id
        
        # Store in vectorstore - make sure store_analysis supports this or write a new method
        store_analysis(
            dag_id=dag_id,
            dag_run_id=state.get("dag_run_id", ""),
            dag_run_date=state.get("dag_run_date", ""),
            logs_analysis=error_snippet,
            proposed_solution=proposed_solution,
            analysis_id=bucket_id,
            actions=actions,
        )
        
        return {"bucket_response": f"Bucket created with ID: {bucket_id}"}
    except Exception as e:
        return {"bucket_response": f"Error creating bucket: {e}"}



def send_analysis_to_slack(state: GraphState) -> Dict[str, str]:
    node_name = "send_analysis_to_slack"
    start_time = time.time()

    analysis_id = state.get("analysis_id", "")
    slack_sent = state.get("slack_sent", False)

    if not analysis_id:
        return {"slack_response": "No analysis ID to fetch from vector store."}

    if slack_sent:
        return {"slack_response": "Slack message already sent."}

    try:
        docs = vectorstore.similarity_search(
            query="", k=1, filter={"analysis_id": analysis_id}
        )  # dummy query

        if not docs:
            return {"slack_response": "No analysis document found."}

        doc = docs[0]
        metadata = doc.metadata
        proposed_solution = metadata.get("proposed_solution", "")
        actions = metadata.get("actions", "")

        if not proposed_solution:
            return {"slack_response": "No proposed solution found in metadata."}

        message = f"Proposed Solution:\n{proposed_solution}"
        if actions:
            message += f"\n\nActions:\n{actions}"

        response = send_to_slack(message=message)

        # Update state to indicate that the Slack message has been sent
        state["slack_sent"] = True
        return {"slack_response": response}
    except Exception as e:
        return {"slack_response": f"Error: {e}"}


def rerun_dag_if_required(state: GraphState) -> Dict[str, str]:
    node_name = "rerun_dag_if_required"
    start_time = time.time()

    # Extract necessary information from the state
    analysis_id = state.get("analysis_id", "")
    dag_id = state.get("dag_name", "")
    dag_run_id = state.get("dag_run_id", "")
    rerun_attempted = state.get("rerun_attempted", False)

    # Check if required information is present
    if not analysis_id or not dag_id:
        return {"rerun_response": "No rerun attempted due to missing information."}

    # Check if a rerun has already been attempted
    if rerun_attempted:
        return {"rerun_response": "Rerun already attempted. Manual support needed."}

    try:
        # Fetch the actions associated with the analysis_id from the vector store
        docs = vectorstore.similarity_search(
            query="", k=1, filter={"analysis_id": analysis_id}
        )

        # Check if any documents were found
        if not docs:
            return {"rerun_response": "No analysis record found."}

        # Retrieve the actions from the first document
        actions = docs[0].metadata.get("actions", "").lower()

        # Check if the actions include a rerun command
        if "rerun" in actions:
            result = rerun_dag(dag_id=dag_id, dag_run_id=dag_run_id)

            # Update state to indicate that the rerun has been attempted
            state["rerun_attempted"] = True

            # Check the result of the rerun
            if "successfully" in result:
                return {"rerun_response": result}
            else:
                return {"rerun_response": "Rerun failed. Manual support needed."}
        else:
            return {"rerun_response": "No rerun action required."}
    except Exception as e:
        return {"rerun_response": f"Error: {e}"}


# Graph
builder = StateGraph(GraphState)

builder.add_node("extract_info", extract_dag_info)
builder.add_node("get_logs", get_logs)
builder.add_node("do_not_fetch_logs", do_not_fetch_logs)
builder.add_node("analyze_logs", analyze_logs)
builder.add_node("send_analysis_to_slack", send_analysis_to_slack)
builder.add_node("rerun_dag_if_required", rerun_dag_if_required)
builder.add_node("bucket_error_analysis", bucket_error_analysis)


builder.add_conditional_edges(
    "extract_info",
    route_logs,
    {
        "get_logs": "get_logs",
        "do_not_fetch_logs": "do_not_fetch_logs",
    },
)

builder.add_edge("get_logs", "analyze_logs")
builder.add_edge("analyze_logs", "bucket_error_analysis")
builder.add_edge("bucket_error_analysis", "send_analysis_to_slack")
builder.add_edge("send_analysis_to_slack", "rerun_dag_if_required")
builder.set_entry_point("extract_info")
graph = builder.compile()
