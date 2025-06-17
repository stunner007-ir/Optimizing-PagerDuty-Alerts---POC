import os
from langgraph.graph import MessageGraph, StateGraph, END
from typing import TypedDict, List, Dict, Any, Optional
import logging
from dotenv import load_dotenv
from langchain_core.messages import BaseMessage, HumanMessage
from langchain_core.prompts import ChatPromptTemplate
from datetime import datetime
import json
import re
from model import model

# Load environment variables and configure logging
load_dotenv()
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


# Define the state schema
class GraphState(TypedDict):
    extracted_logs: Optional[str]
    analysis_results: Optional[str]
    proposed_solution: Optional[str]
    dag_name: Optional[str]
    dag_run_id: Optional[str]
    dag_run_date: Optional[str]
    error_message: Optional[str]
    actions: Optional[str]
    cached_analysis: Optional[str]


# Model Binding

import re
from typing import Dict


def extract_dag_info(log_line: str) -> Dict[str, str]:
    """
    Extracts DAG name, run ID, and run date from a log line with the specified format.

    Args:
        log_line (str): A log line containing the DAG information.

    Returns:
        Dict[str, str]: A dictionary containing the extracted DAG name, run ID,
                         and run date.  Returns "Unknown" for any missing information.
    """

    dag_name = "Unknown_DAG"
    dag_run_id = "Unknown_RunID"
    dag_run_date = "Unknown_RunDate"

    # Use a single regex to extract all components
    match = re.search(r"dag_id=(?P<dag_name>[^/]+)/run_id=(?P<run_id>[^/]+)/", log_line)

    if match:
        dag_name = match.group("dag_name")
        dag_run_id = match.group("run_id")

        # Further extract the date from the run_id (if possible)
        date_match = re.search(r"(\d{4}-\d{2}-\d{2})", dag_run_id)  # e.g., 2025-06-09
        if date_match:
            dag_run_date = date_match.group(1)  # Extract just the date part

    return {
        "dag_name": dag_name,
        "dag_run_id": dag_run_id,
        "dag_run_date": dag_run_date,
    }


def analyze_logs(state: Dict[str, Any]) -> Dict[str, str]:
    logger.info("=== NODE: analyze_logs - Analyzing logs with LLM ===")
    """Analyze logs and generate analysis results"""

    extracted_logs = state.get("extracted_logs", "")
    dag_id = state.get("dag_name", "")

    # First, extract error message from the logs
    error_message = "No specific error message found in logs."
    error_match = re.search(
        r"(ERROR:|Error:)\s*(.+)", extracted_logs
    )  # Adjust regex as needed
    if error_match:
        error_message = error_match.group(2).strip()

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

        return {
            "analysis_results": analysis,
            "proposed_solution": solution,
            "error_message": error_message,  # Pass the extracted error message
        }
    except Exception as e:
        return {
            "analysis_results": f"Failed: {e}",
            "proposed_solution": "",
            "error_message": f"Failed to extract error: {e}",
        }


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
    llm = prompt | model

    def generate_rca(state: Dict[str, Any]) -> Dict[str, Any]:
        extracted_logs = state.get("extracted_logs")
        if not extracted_logs:
            return {
                "analysis_results": "Error: Airflow logs not found in state.",
                "proposed_solution": "",
                "error_message": "No logs to process",
            }

        try:
            # Map state fields to RCA format
            root_cause = state.get("analysis_results", "N/A")
            resolution = state.get("proposed_solution", "N/A")
            error_message = state.get(
                "error_message", "N/A"
            )  # Get error message from state

            dag_name = state.get("dag_name", "N/A")
            dag_run_id = state.get("dag_run_id", "N/A")
            dag_run_date = state.get("dag_run_date", "N/A")
            task_id = "N/A"  # not available in current state
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

    2. Summary of the Issue
    {error_message}

    3. Affected DAG ID
    {dag_name}

    4. Affected Task ID
    {task_id}

    5. Execution Date / Run ID
    {dag_run_date} / {dag_run_id}

    6. Logs Summary (Key Error Messages)
    {error_message}

    7. Investigation Steps Taken
    {investigation}

    8. Root Cause
    {root_cause}

    9. Resolution / Fix Applied
    {resolution}

    10. Prevention Measures / Action Items
    {actions}
    """

            # Save RCA file
            os.makedirs("rca", exist_ok=True)
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"rca/{dag_name}_{dag_run_id}_{timestamp}.txt".replace(" ", "_")

            with open(filename, "w") as f:
                f.write(rca_content)

            return {
                "analysis_results": root_cause,
                "proposed_solution": resolution,
                "rca_file": filename,
                "error_message": error_message,  # Pass the error message to the final state
            }

        except Exception as e:
            return {
                "analysis_results": f"Error generating RCA: {e}",
                "proposed_solution": "",
                "error_message": f"Error in RCA generation: {e}",
            }

    return generate_rca


# ============================================
# GRAPH CONSTRUCTION
# ============================================

# GRAPH
builder = StateGraph(GraphState)
builder.set_entry_point("analyze_logs")

# Add all nodes
builder.add_node("analyze_logs", analyze_logs)
builder.add_node("generate_rca", create_rca_node())  # Add the RCA node

# Add edges
builder.add_edge("analyze_logs", "generate_rca")

# Add END edges for terminal nodes
builder.add_edge("generate_rca", END)

# Compile the graph
graph = builder.compile()


def run_agent(log_file_path: str):
    """
    Runs the agent to analyze logs and generate an RCA report.

    Args:
        log_file_path (str): The path to the log file.
    """
    try:
        # Read the log file
        with open(log_file_path, "r") as f:
            logs = f.read()

        # Extract DAG information from logs
        dag_info = extract_dag_info(logs)

        # Prepare the initial state for the agent
        initial_state = {
            "extracted_logs": logs,
            "dag_name": dag_info["dag_name"],
            "dag_run_id": dag_info["dag_run_id"],
            "dag_run_date": dag_info["dag_run_date"],
            # "error_message": error_message,  # No longer passing it directly
        }

        # Run the agent
        result = graph.invoke(initial_state)

        # Print the result (or handle it as needed)
        print("Agent execution completed!")
        print("Result:", json.dumps(result, indent=2))

    except FileNotFoundError:
        print(f"Error: Log file not found at {log_file_path}")
    except Exception as e:
        print(f"An error occurred: {e}")


if __name__ == "__main__":
    # Example usage:
    log_file = "testing/log3.log"  # Replace with your log file

    run_agent(log_file)
