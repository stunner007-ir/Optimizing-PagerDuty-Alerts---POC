import requests
from requests.auth import HTTPBasicAuth
import json
import os
from dotenv import load_dotenv
from langchain_core.tools import Tool
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

AIRFLOW_URL = os.getenv("AIRFLOW_URL")
AIRFLOW_API_USERNAME = os.getenv("AIRFLOW_API_USERNAME")
AIRFLOW_API_PASSWORD = os.getenv("AIRFLOW_API_PASSWORD")
SLACK_BOT_TOKEN = os.getenv("SLACK_BOT_TOKEN")
SLACK_CHANNEL_ID = os.getenv("SLACK_CHANNEL_ID")


def fetch_log_details(dag_name: str) -> dict:
    """
    Fetches logs for all tasks in the most recent DAG run of a given Airflow DAG.

    Args:
        dag_name (str): The ID of the Airflow DAG.

    Returns:
        str: A JSON string containing a dictionary of logs, keyed by task_id.
             Returns an error message as a string if fetching fails.
    """
    dag_runs_url = f"{AIRFLOW_URL}/api/v1/dags/{dag_name}/dagRuns"
    try:
        response = requests.get(
            dag_runs_url, auth=(AIRFLOW_API_USERNAME, AIRFLOW_API_PASSWORD)
        )
        response.raise_for_status()
        dag_runs = response.json().get("dag_runs", [])

        # Sort dag_runs by execution_date in descending order to get the most recent run first
        dag_runs.sort(key=lambda x: x.get("execution_date"), reverse=True)

        logs = {}

        if not dag_runs:
            return "No DAG runs found."

        dag_run = dag_runs[0]  # Get the most recent DAG run
        dag_run_id = dag_run.get("dag_run_id", "unknown")
        task_instances_url = (
            f"{AIRFLOW_URL}/api/v1/dags/{dag_name}/dagRuns/{dag_run_id}/taskInstances"
        )
        task_response = requests.get(
            task_instances_url, auth=(AIRFLOW_API_USERNAME, AIRFLOW_API_PASSWORD)
        )
        task_response.raise_for_status()
        task_instances = task_response.json().get("task_instances", [])

        for task in task_instances:
            task_id = task.get("task_id", "unknown")
            task_try_number = task.get(
                "try_number", 1
            )  # Default to the first try if not specified
            logs_url = f"{AIRFLOW_URL}/api/v1/dags/{dag_name}/dagRuns/{dag_run_id}/taskInstances/{task_id}/logs/{task_try_number}"

            # Fetch logs
            log_response = requests.get(
                logs_url, auth=(AIRFLOW_API_USERNAME, AIRFLOW_API_PASSWORD)
            )

            # Check if the response is valid JSON
            try:
                log_response.raise_for_status()
                if log_response.headers.get("Content-Type") == "application/json":
                    log_data = log_response.json()
                    print(log_data)
                    logs[task_id] = log_data.get("logs", "No logs found.")
                else:
                    # If not JSON, return the raw text
                    logs[task_id] = log_response.text
            except json.JSONDecodeError as json_err:
                logs[task_id] = f"Error fetching logs: {json_err}"
            except Exception as e:
                logs[task_id] = f"Error fetching logs for task {task_id}: {e}"

        return logs  # Return a dictionary of logs keyed by task_id
    except Exception as e:
        logger.exception(f"Error fetching logs for DAG {dag_name}: {e}")
        return {}


def send_to_slack(message: str) -> str:
    """
    Sends a message to a specific Slack channel.

    Args:
        message (str): The message to send to Slack.

    Returns:
        str: A message indicating whether the message was sent successfully or if there was an error.
    """
    slack_bot_token = SLACK_BOT_TOKEN
    slack_channel_id = SLACK_CHANNEL_ID


    if not slack_bot_token or not slack_channel_id:
        return "Error: SLACK_BOT_TOKEN or SLACK_CHANNEL_ID not set in environment variables."

    url = "https://slack.com/api/chat.postMessage"
    headers = {
        "Authorization": f"Bearer {slack_bot_token}",
        "Content-Type": "application/json; charset=utf-8",
    }
    data = {
        "channel": slack_channel_id,
        "text": message,
    }
    try:
        response = requests.post(url, headers=headers, data=json.dumps(data))
        response.raise_for_status()
        result = response.json()
        if result.get("ok"):
            return f"Sent Message: {message}"
        else:
            return f"Error sending message to Slack: {result.get('error')}"
    except requests.exceptions.RequestException as e:
        return f"Error sending message to Slack: {e}"


def rerun_dag(dag_id: str, dag_run_id: str) -> str:
    """Triggers a DAG run via Airflow REST API"""
    # url = f"{AIRFLOW_URL}/api/v1/dags/{dag_id}/dagRuns"
    url = f"{AIRFLOW_URL}/api/v1/dags/{dag_id}/dagRuns/{dag_run_id}/clear"

    # if dag_run_id is None:
    # dag_run_id = "agent__" + datetime.now().strftime("%Y%m%dT%H%M%S")

    print(f"Rerunning DAG: {dag_id} with run_id: {dag_run_id}")

    payload = {
        # "dag_run_id": dag_run_id,
        "dry_run": False,
    }
    try:
        response = requests.post(
            url, json=payload, auth=(AIRFLOW_API_USERNAME, AIRFLOW_API_PASSWORD)
        )
        # response.raise_for_status()
        dag_run = response.json()
        logger.info(f"Triggered DAG run: {dag_run}")
        return f"DAG '{dag_id}' rerun successfully with run_id: {dag_run.get('dag_run_id', 'unknown')}"
    except Exception as e:
        logger.exception(f"Failed to rerun DAG {dag_id}")
        return f"Error rerunning DAG: {e}"


tools = [
    Tool(
        name="fetch_log_details",
        func=fetch_log_details,
        description="Fetches Airflow logs for a given DAG name.",
    ),
    Tool(
        name="send_to_slack",
        func=send_to_slack,
        description="Sends a message to the Slack channel.",
    ),
    Tool(
        name="rerun_dag",
        func=rerun_dag,
        description="Triggers a DAG run via Airflow REST API.",
    ),
]
