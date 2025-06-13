import json
import os
import logging
from typing import Dict
from urllib.parse import urlparse, parse_qs
import re

logger = logging.getLogger(__name__)


def load_existing_events(filename: str):
    if os.path.exists(filename):
        try:
            with open(filename, "r") as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"Failed to read {filename}: {e}")
    return []


def save_as_json(data, filename: str):
    try:
        with open(filename, "w") as f:
            json.dump(data, f, indent=2)
    except Exception as e:
        logger.error(f"Error saving to {filename}: {e}")


def parse_slack_text(text: str) -> Dict:
    """Parses Slack message text and extracts Airflow alert info for both DAG and Task failures."""
    try:
        # Normalize text: remove leading/trailing whitespace
        cleaned_text = text.strip()

        # Determine if it's a DAG failure or a Task failure based on the text content
        is_task_failure = "Task" in cleaned_text

        if is_task_failure:
            # Extract Task Name
            task_name_match = re.search(r"Task \*(.*?)\* failed", cleaned_text)
            task_name = task_name_match.group(1) if task_name_match else None

            # Extract DAG name
            dag_name_match = re.search(r"DAG: \*(.*?)\*", cleaned_text)
            dag_name = dag_name_match.group(1) if dag_name_match else None

            # Extract Run Date
            run_date_match = re.search(r"Run Date: \*(.*?)\*", cleaned_text)
            run_date = run_date_match.group(1) if run_date_match else None

            # Extract Status
            status = "failed"  # Task failures are always 'failed' in this context

            # Extract Log URL
            log_url_match = re.search(r"Log URL:\*<(.*?)>", cleaned_text)
            log_url = log_url_match.group(1) if log_url_match else None

            parsed_url = urlparse(log_url)
            query_params = parse_qs(parsed_url.query)

            run_id = query_params.get('dag_run_id')[0]

            return {
                "dag_name": dag_name,
                "task_name": task_name,
                "run_date": run_date,
                "run_id": run_id,
                "dag_status": status,
                "log_url": log_url,
                "type": "task_failure",
                "full_text": text,
            }
        else:
            # DAG Failure Parsing
            # Extract DAG name
            dag_name_match = re.search(r"DAG \*(.*?)\* failed", cleaned_text)
            dag_name = dag_name_match.group(1) if dag_name_match else None

            # Extract Run ID
            run_id_match = re.search(r"Run ID: \*(.*?)\*", cleaned_text)
            run_id = run_id_match.group(1) if run_id_match else None

            # Extract Run Date
            run_date_match = re.search(r"Run Date: \*(.*?)\*", cleaned_text)
            run_date = run_date_match.group(1) if run_date_match else None

            # Extract Status (based on presence of "failed!" or "succeeded!")
            status = "failed"  # DAG failures are always failed in this context

            return {
                "dag_name": dag_name,
                "run_id": run_id,
                "run_date": run_date,
                "dag_status": status,
                "type": "dag_failure",
                "full_text": text,
            }

    except Exception as e:
        logger.error(f"Error parsing Slack text: {e}")
        return {"error": str(e), "full_text": text}



# Utility function to normalize logs for better similarity matching
def normalize_error_logs(logs: str) -> str:
    """Normalize logs by removing timestamps, IDs, and other variable content"""
    import re

    # Remove timestamps (various formats)
    normalized = re.sub(
        r"\d{4}-\d{2}-\d{2}[T\s]\d{2}:\d{2}:\d{2}[\.\d]*[Z]?", "<TIMESTAMP>", logs
    )
    normalized = re.sub(
        r"\d{2}/\d{2}/\d{4}\s\d{2}:\d{2}:\d{2}", "<TIMESTAMP>", normalized
    )

    # Remove UUIDs
    normalized = re.sub(
        r"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}",
        "<UUID>",
        normalized,
    )

    # Remove run IDs (assuming they follow a pattern)
    normalized = re.sub(r"run_id[:\s]*[\w\-]+", "run_id: <RUN_ID>", normalized)
    normalized = re.sub(r"task_id[:\s]*[\w\-]+", "task_id: <TASK_ID>", normalized)

    # Remove file paths but keep the file name
    normalized = re.sub(r"(/[\w\-\.]+)+/([^/\s]+)", r"<PATH>/\2", normalized)

    # Remove line numbers
    normalized = re.sub(r"line\s*\d+", "line <LINE_NUM>", normalized)

    # Remove memory addresses
    normalized = re.sub(r"0x[0-9a-fA-F]+", "<MEMORY_ADDR>", normalized)

    # Normalize whitespace
    normalized = " ".join(normalized.split())

    return normalized