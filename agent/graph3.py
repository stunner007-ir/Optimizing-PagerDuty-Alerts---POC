from typing import TypedDict, List, Dict, Optional, Literal, Any
import time
import uuid
import json
import logging
import os
from datetime import datetime
from dotenv import load_dotenv
from langgraph.graph import StateGraph, END
from langchain_core.messages import BaseMessage, HumanMessage
from agent.tools.tools import tools, fetch_log_details, send_to_slack, rerun_dag
from db.vector_store import store_analysis, vectorstore
from db.error_categoryDB import store_error_categories, retrieve_similar_error_action
from agent.model import model
from utilities.utils import extract_errors_from_log
from langchain_core.prompts import ChatPromptTemplate
from langchain_openai import ChatOpenAI
from typing import Dict, Any
from rca.rca import generate_rca_pdf

# Load env and setup logging
load_dotenv()
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


# State Schema
class GraphState(TypedDict):
    messages: List[BaseMessage]
    dag_name: Optional[str]
    dag_run_date: Optional[str]
    dag_run_id: Optional[str]
    log_url: Optional[str]
    logs: Optional[str]
    extracted_logs: Optional[str]
    analysis_results: Optional[str]
    solution: Optional[str]
    dag_status: Optional[str]
    user_message: Optional[str]
    llm_response: Optional[str]
    analysis_id: Optional[str]
    slack_sent: Optional[bool]
    rerun_attempted: Optional[bool]
    proposed_solution: Optional[str]
    validated: Optional[str]
    bucket_response: Optional[str]
    slack_response: Optional[str]
    rerun_response: Optional[str]
    final_status: Optional[str]
    dag: Optional[str]
    status: Optional[str]
    cached_analysis_id: Optional[str]
    cached_solution: Optional[str]
    cached_analysis: Optional[str]
    cached_actions: Optional[str]
    is_cached: Optional[bool]
    similarity_score: Optional[float]  # Add similarity score
    error_category: Optional[str]
    error_message: Optional[str]
    error_type: Optional[str]
    actions: Optional[str]
    action_state: Optional[str]


# Model Binding
model_with_tools = model.bind_tools(tools)


# 1. Entry Point - Extract DAG Information
def extract_dag_info(state: GraphState) -> Dict[str, any]:
    logger.info("=== NODE: extract_dag_info - Starting DAG info extraction ===")
    messages = state.get("messages", {})
    text_details = messages.get("text_details", {})
    print(f"Extracting DAG info from: {text_details}")
    if isinstance(text_details, str):
        try:
            text_details = json.loads(text_details)
        except json.JSONDecodeError:
            text_details = {}
    return {
        "dag_name": text_details.get("dag_name"),
        "dag_run_id": text_details.get("run_id"),
        "dag_run_date": text_details.get("run_date"),
        "dag_status": text_details.get("dag_status"),
        "log_url": text_details.get("log_url"),
        "user_message": text_details.get("full_text"),
        "slack_sent": False,
        "rerun_attempted": False,
        "is_cached": False,
    }


# 2. Validate DAG Information
def validate_dag_info(state: GraphState) -> Dict[str, str]:
    logger.info("=== NODE: validate_dag_info - Validating DAG information ===")
    logger.info("Validating DAG information...")
    if state.get("dag_name") and state.get("dag_run_id") and state.get("dag_status"):
        return {"validated": "true"}
    else:
        return {"validated": "false"}


# 3. Routing Function After Validation
def route_validate_result(state: GraphState) -> Literal["route_logs", "exit"]:
    """Routing function with proper return type annotation"""
    return "route_logs" if state.get("validated") == "true" else "exit"


# 4a. Exit Node (if validation fails)
def exit_node(state: GraphState) -> Dict[str, str]:
    logger.info("=== NODE: exit - Exiting due to missing DAG info ===")
    logger.info("Exiting due to missing DAG info.")
    return {"status": "exited"}


# 4b. Route Logs Node (if validation succeeds)
def route_logs_node(state: GraphState) -> Dict[str, any]:
    logger.info("=== NODE: route_logs - Routing based on DAG status ===")
    return {}


# 5. Routing Function for Logs
def route_logs_func(state: GraphState) -> Literal["get_logs", "do_not_fetch_logs"]:
    """Routing function with proper return type annotation"""
    dag_status = state.get("dag_status")
    if dag_status == "failed":
        if state.get("dag_name") and state.get("dag_run_id"):
            return "get_logs"
    return "do_not_fetch_logs"


# 6a. Get Logs (if DAG failed)
def get_logs(state: GraphState) -> Dict[str, str]:
    logger.info("=== NODE: get_logs - Fetching DAG logs ===")
    try:
        logger.info(
            "Fetching logs for DAG: %s, Run ID: %s",
            state["dag_name"],
            state["dag_run_id"],
        )
        logs = fetch_log_details(state["dag_name"])
        # print(f"Fetched logs: {logs}...")
        extracted_logs = extract_errors_from_log(logs)
        logger.info("Extracted Logs: %s", extracted_logs)

        return {"logs": logs, "extracted_logs": extracted_logs}
    except Exception as e:
        return {"logs": f"Error: {e}", "extracted_logs": ""}


def search_error_category(state: Dict[str, Any]) -> Dict[str, Any]:
    """
    Search for error category based on logs and update the action state.
    """
    logger.info("=== NODE: search_error_category - Searching for error category ===")

    extracted_logs = state.get("extracted_logs", "")

    if not extracted_logs:
        logger.warning("No logs to analyze. Returning default 'No logs to analyze'.")
        return {"error_category": "No logs to analyze"}

    try:
        # Use the extracted logs as the query
        if isinstance(extracted_logs, list):
            query_text = "\n".join(extracted_logs)  # Join list elements into a string
        else:
            query_text = extracted_logs

        similar_docs = retrieve_similar_error_action(query_text, k=3)

        if similar_docs:
            # Assuming the first result is the best match
            best_match = similar_docs[0]
            metadata = best_match.metadata
            error_category = metadata.get("error_category", "Unknown")
            error_type = metadata.get("error_type", "Unknown")
            actions = metadata.get("actions", "No actions defined")

            logger.info(
                f"Found error category: {error_category}, Error Type: {error_type}, Actions: {actions}"
            )

            # Construct the result dictionary
            result = {
                "error_message": best_match.page_content,
                "error_category": error_category,
                "error_type": error_type,
                "actions": actions,
                # "similarity_score": best_match.score, #similarity score is not an attribute of best_match object
            }

            # Update the state with the actions
            state["action_state"] = actions
            logger.info(f"Updated action_state in GraphState with: {actions}")

            return result
        else:
            logger.warning("No matching category found.")
            return {"error_category": "No matching category found"}

    except Exception as e:
        logger.exception(f"Error searching category: {e}")  # Log the full exception
        return {"error_category": f"Error searching category: {e}"}


# 6b. Do Not Fetch Logs (if DAG didn't fail)
def do_not_fetch_logs(state: GraphState) -> Dict[str, str]:
    logger.info("=== NODE: do_not_fetch_logs - Skipping log fetch ===")
    return {"logs": "No logs to fetch.", "normalized_logs": ""}


# 7. Check Error Bucket for Similar Errors
def check_error_bucket(state: GraphState) -> Dict[str, any]:
    logger.info("=== NODE: check_error_bucket - Checking for similar errors ===")
    """Check for similar errors based on normalized logs"""

    extracted_logs = state.get("extracted_logs", "")
    logs = state.get("logs", "")
    dag_name = state.get("dag_name", "")

    if not extracted_logs and logs:
        extracted_logs = extract_errors_from_log(logs)

    query_text = (
        extracted_logs if isinstance(extracted_logs, str) else "\n".join(extracted_logs)
    )

    if not extracted_logs:
        logger.warning("No logs to check against")
        return {"is_cached": False}

    try:
        # Search for similar errors using normalized logs
        # Use a higher k value to get more candidates
        similar_docs = vectorstore.similarity_search_with_score(
            query_text,
            k=5,  # Get top 5 similar errors
            filter=(
                {"dag_name": dag_name} if dag_name else None
            ),  # Filter by DAG name if available
        )

        if similar_docs:
            # Find the best match based on similarity score
            best_match = None
            best_score = 0.0

            for doc, score in similar_docs:
                # Adjust threshold based on your needs (0.8 = 80% similarity)
                if score > 0.8:  # High similarity threshold
                    if score > best_score:
                        best_match = doc
                        best_score = score

            if best_match:
                metadata = best_match.metadata
                cached_solution = metadata.get("solution", "")
                cached_actions = metadata.get("actions", "")
                cached_analysis_id = metadata.get("analysis_id", "")
                cached_analysis = metadata.get("analysis", "")

                # Verify we have valid cached data
                if cached_solution and cached_actions:
                    logger.info(
                        f"Found cached solution with analysis_id: {cached_analysis_id}, "
                        f"similarity score: {best_score:.2f}"
                    )

                    # Check if the cached solution is recent (optional)
                    # You might want to add timestamp checking here

                    return {
                        "cached_analysis_id": cached_analysis_id,
                        "cached_solution": cached_solution,
                        "cached_actions": cached_actions,
                        "cached_analysis": cached_analysis,
                        "proposed_solution": cached_solution,
                        "analysis_id": cached_analysis_id,
                        "is_cached": True,
                        "similarity_score": best_score,
                    }
                else:
                    logger.info(f"Found similar error but missing solution/actions")

        logger.info("No similar cached solutions found")
    except Exception as e:
        logger.warning(f"Error checking for cached solutions: {e}")

    return {"is_cached": False}


# 8. Routing After Check
def route_after_check(
    state: GraphState,
) -> Literal["analyze_logs", "generate_rca"]:  # Route to RCA instead of slack
    """Route based on whether we found a cached solution"""
    if state.get("is_cached") and state.get("cached_solution"):
        # Skip analysis if we have a good cached solution
        return "generate_rca"  # Route to RCA generation

    return "analyze_logs"


# 9. Analyze Logs (if no cached solution)
def analyze_logs(state: GraphState) -> Dict[str, str]:
    logger.info("=== NODE: analyze_logs - Analyzing logs with LLM ===")
    """Analyze logs and generate analysis results"""

    # Skip analysis if we have a cached solution
    if state.get("is_cached") and state.get("cached_solution"):
        logger.info("Using cached analysis, skipping LLM analysis")
        return {
            "analysis_results": state.get("cached_analysis", ""),
            "proposed_solution": state.get("cached_solution", ""),
            "analysis_id": state.get("cached_analysis_id", ""),
        }

    extracted_logs = state.get("extracted_logs", "")
    dag_id = state.get("dag_name", "")

    prompt = (
        f"Please analyze the following Errors from the logs from DAG '{dag_id}':\n\n{extracted_logs}\n\n and generate proper root cause analysis and a proposed solution. don't miss any thing\n"
        "Respond in the following format:\n"
        "Analysis: <your analysis>\n"
        "Solution: <your proposed solution>"
    )

    try:
        response = model.invoke([HumanMessage(content=prompt)]).content
        analysis, solution = "", ""
        if "Solution:" in response:
            parts = response.split("Solution:")
            analysis = parts[0].replace("Analysis:", "").strip()
            solution = parts[1].strip()

        analysis_id = str(uuid.uuid4())
        return {
            "analysis_results": analysis,
            "proposed_solution": solution,
            "analysis_id": analysis_id,
        }
    except Exception as e:
        return {"analysis_results": f"Failed: {e}"}


# 10. Bucket Error Analysis (store new analysis)
def bucket_error_analysis(state: GraphState) -> Dict[str, str]:
    logger.info("=== NODE: bucket_error_analysis - Storing new error analysis ===")
    """Store new analysis in the error bucket"""

    # Skip if this is a cached solution
    if state.get("is_cached"):
        return {"bucket_response": "Using cached solution, skipping storage"}

    logs = state.get("logs", "")
    extracted_logs = state.get("extracted_logs", "")
    dag_id = state.get("dag_name", "")
    run_id = state.get("dag_run_id", "")
    run_date = state.get("dag_run_date", "")
    analysis_id = state.get("analysis_id", str(uuid.uuid4()))
    proposed_solution = state.get("proposed_solution", "")
    analysis = state.get("analysis_results", "")
    actions = "rerun" if not state.get("rerun_attempted", False) else "manual_check"

    if not logs or not dag_id or not proposed_solution:
        return {"bucket_response": "Insufficient data."}

    try:
        # Use normalized logs for better similarity matching
        if not extracted_logs:
            extracted_logs = extract_errors_from_log(logs)

        error_signature = extracted_logs[:500]
        related_dags = [dag_id]

        metadata = {
            "error_signature": error_signature,
            "analysis": analysis,
            "solution": proposed_solution,
            "actions": actions,
            "related_dags": related_dags,
            "analysis_id": analysis_id,
            "composite_id": f"{dag_id}::{run_id}::{run_date}",
            "dag_name": dag_id,
            "timestamp": time.time(),  # Add timestamp for recency checks
        }

        # Store using normalized logs for vector embedding
        store_analysis(
            dag_id,
            run_id,
            run_date,
            logs_analysis=analysis,
            proposed_solution=proposed_solution,
            analysis_id=analysis_id,
            actions=actions,
            metadata=metadata,
        )

        return {"bucket_response": f"Stored with ID {analysis_id}"}
    except Exception as e:
        return {"bucket_response": f"Bucket error: {e}"}


def create_rca_node():
    prompt_text = """You are an expert in Airflow and debugging complex systems.
    Analyze the following Airflow logs to determine the root cause of the failure.
    Present your findings in the following format:

    **Root Cause:** [A concise description of the root cause]

    **Contributing Factors:** [A bulleted list of factors that contributed to the failure]

    **Steps to Reproduce:** [Detailed steps to reproduce the issue]

    **Resolution:** [Specific steps to resolve the issue and prevent it from recurring]

    **Logs:**
    {logs}
    """

    prompt = ChatPromptTemplate.from_template(prompt_text)
    llm = prompt | model_with_tools
    return llm


llm = create_rca_node()


def generate_rca(state: Dict[str, Any]) -> Dict[str, Any]:
    extracted_logs = state.get("extracted_logs")
    if not extracted_logs:
        return {
            "analysis_results": "Error: Airflow logs not found in state.",
            "proposed_solution": "",
        }

    try:
        rca_response = llm.invoke({"logs": extracted_logs})
        rca_text = rca_response.content

        # Parse LLM response
        if "**Root Cause:**" in rca_text and "**Resolution:**" in rca_text:
            try:
                root_cause_start = rca_text.find("**Root Cause:**") + len(
                    "**Root Cause:**"
                )
                contributing_factors_start = rca_text.find("**Contributing Factors:**")
                if contributing_factors_start == -1:
                    contributing_factors_start = len(rca_text)
                root_cause = rca_text[
                    root_cause_start:contributing_factors_start
                ].strip()

                resolution_start = rca_text.find("**Resolution:**") + len(
                    "**Resolution:**"
                )
                resolution = rca_text[resolution_start:].strip()
            except Exception:
                root_cause = rca_text
                resolution = "Could not parse resolution accurately."
        else:
            root_cause = rca_text
            resolution = "Could not parse resolution accurately."

        # Map state fields to RCA format
        dag_name = state.get("dag_name", "N/A")
        dag_run_id = state.get("dag_run_id", "N/A")
        dag_run_date = state.get("dag_run_date", "N/A")
        task_id = "N/A"  # not available in current state
        analysis_results = state.get("analysis_results", "N/A")
        error_message = state.get("error_message", "N/A")
        investigation = state.get("cached_analysis") or state.get(
            "analysis_results", "N/A"
        )
        actions = state.get("actions") or resolution

        # Use current timestamp as incident time
        incident_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # Format final RCA content
        rca_content = f"""RCA Report

1. Date & Time of Incident
{incident_time}

2. Affected DAG ID
{dag_name}

3. Execution Date / Run ID
{dag_run_date} / {dag_run_id}

4. Logs Summary (Key Error Messages)
{error_message}

5. Investigation Steps Taken
{investigation}

6. Root Cause
{root_cause}

7. Resolution / Fix Applied
{resolution}

8. Prevention Measures / Action Items
{actions}
"""

        # Save RCA file
        os.makedirs("rca/rca_pdf", exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"rca/rca_raw/{dag_name}_{dag_run_id}_{timestamp}.txt".replace(" ", "_")

        with open(filename, "w") as f:
            f.write(rca_content)

        generate_rca_pdf(
            input_txt_file=filename,
            output_pdf_file=f"rca/rca_pdf/rca_report_{dag_name}_{dag_run_id}_{timestamp}.pdf",
        )
        return {
            "analysis_results": root_cause,
            "proposed_solution": resolution,
            "rca_file": filename,
        }

    except Exception as e:
        return {
            "analysis_results": f"Error generating RCA: {e}",
            "proposed_solution": "",
        }


# 11. Send Analysis to Slack
def send_analysis_to_slack(state: GraphState) -> Dict[str, any]:
    logger.info("=== NODE: send_analysis_to_slack - Sending notification to Slack ===")

    if state.get("slack_sent"):
        return {"slack_response": "Already sent"}

    try:
        solution = state.get("proposed_solution", "")
        actions = state.get("cached_actions", "")
        is_cached = state.get("is_cached", False)
        similarity_score = state.get("similarity_score", 0.0)

        if not solution:
            return {"slack_response": "No solution to send"}

        message = f"DAG: {state.get('dag_name')}\n"
        message += f"Run ID: {state.get('dag_run_id')}\n"

        if is_cached:
            message += (
                f"Using cached solution (ID: {state.get('cached_analysis_id')})\n"
            )
            message += f"Similarity score: {similarity_score:.2%}\n"
        else:
            message += "New error analysis\n"

        message += f"\nProposed Solution:\n{solution}\n\n"

        if actions:
            message += f"Recommended Actions:\n{actions}"

        response = send_to_slack(message=message)
        return {"slack_response": response, "slack_sent": True}
    except Exception as e:
        return {"slack_response": f"Slack send failed: {e}"}


# 12. Rerun DAG if Required
def rerun_dag_if_required(state: GraphState) -> Dict[str, any]:
    logger.info("=== NODE: rerun_dag_if_required - Checking if DAG rerun needed ===")

    if state.get("rerun_attempted", False):
        return {"rerun_response": "Already rerun attempted"}

    try:
        # Get actions from state or cached actions
        actions = state.get("cached_actions", "")

        if not actions:
            return {"rerun_response": "No actions found"}

        actions_lower = actions.lower()
        if "rerun" in actions_lower:
            result = rerun_dag(state["dag_name"], state["dag_run_id"])
            return {"rerun_response": result, "rerun_attempted": True}
        else:
            return {"rerun_response": f"Action required: {actions}"}
    except Exception as e:
        return {"rerun_response": f"Rerun failed: {e}"}


# 13. Summarize Outcome (Final Node)
def summarize_outcome(state: GraphState) -> Dict[str, str]:
    logger.info("=== NODE: summarize_outcome - Creating final summary ===")

    is_cached = state.get("is_cached", False)
    similarity_score = state.get("similarity_score", 0.0)

    status_details = []
    if is_cached:
        status_details.append(
            f"Used cached solution (similarity: {similarity_score:.2%})"
        )
    if state.get("rerun_attempted"):
        status_details.append("DAG rerun attempted")

    final_status = (
        " - ".join(status_details) if status_details else "Manual attention needed"
    )

    return {
        "final_status": final_status,
        "dag": state.get("dag_name"),
        "analysis_id": state.get("analysis_id"),
    }


# ============================================
# GRAPH CONSTRUCTION
# ============================================

# GRAPH
builder = StateGraph(GraphState)
builder.set_entry_point("extract_info")

# Add all nodes
builder.add_node("extract_info", extract_dag_info)
builder.add_node("validate_dag_info", validate_dag_info)
builder.add_node("exit", exit_node)
builder.add_node("route_logs", route_logs_node)
builder.add_node("get_logs", get_logs)
builder.add_node("generate_rca", generate_rca)  # Add the RCA node
builder.add_node("search_error_category", search_error_category)
builder.add_node("do_not_fetch_logs", do_not_fetch_logs)
builder.add_node("check_error_bucket", check_error_bucket)
builder.add_node("analyze_logs", analyze_logs)
builder.add_node("bucket_error_analysis", bucket_error_analysis)
builder.add_node("send_analysis_to_slack", send_analysis_to_slack)
builder.add_node("rerun_dag_if_required", rerun_dag_if_required)
builder.add_node("summarize_outcome", summarize_outcome)

# Add edges
builder.add_edge("extract_info", "validate_dag_info")

# Add conditional edges
builder.add_conditional_edges(
    "validate_dag_info",
    route_validate_result,
    {"route_logs": "route_logs", "exit": "exit"},
)

builder.add_conditional_edges(
    "route_logs",
    route_logs_func,
    {"get_logs": "get_logs", "do_not_fetch_logs": "do_not_fetch_logs"},
)

# Modified flow: get logs -> search for error category -> check error bucket -> analyze if needed -> RCA -> slack
builder.add_edge("get_logs", "search_error_category")
builder.add_edge("search_error_category", "check_error_bucket")

builder.add_conditional_edges(
    "check_error_bucket",
    route_after_check,
    {
        "analyze_logs": "analyze_logs",
        "generate_rca": "generate_rca",  # Route to RCA if cached
    },
)

builder.add_edge(
    "analyze_logs", "bucket_error_analysis"
)  # store new analysis if not cached
builder.add_edge(
    "bucket_error_analysis", "generate_rca"
)  # RCA after new analysis is bucketed

builder.add_edge("generate_rca", "send_analysis_to_slack")  # RCA to slack

builder.add_edge("send_analysis_to_slack", "rerun_dag_if_required")
builder.add_edge("rerun_dag_if_required", "summarize_outcome")
builder.add_edge("do_not_fetch_logs", "summarize_outcome")

# Add END edges for terminal nodes
builder.add_edge("exit", END)
builder.add_edge("summarize_outcome", END)

# Compile the graph
graph3 = builder.compile()
