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

            run_id = query_params.get("dag_run_id")[0]

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


def extract_errors_from_log(log_content: str | dict) -> list:
    """Extract error messages from log content"""

    # Handle both string and dictionary inputs
    if isinstance(log_content, dict):
        # If it's a dictionary, concatenate all log values
        all_logs = []
        for task_name, task_log in log_content.items():
            if isinstance(task_log, str):
                all_logs.append(f"=== Task: {task_name} ===\n{task_log}")
        log_content = "\n\n".join(all_logs)

    # If log_content is empty or None, return empty list
    if not log_content:
        return []

    error_patterns = [
        # General error patterns
        r"(?i)error[:\s].*?(?=\n|$)",
        r"(?i)exception[:\s].*?(?=\n|$)",
        r"(?i)failed[:\s].*?(?=\n|$)",
        r"(?i)traceback.*?(?=\n\n|\Z)",
        r"(?i)\[error\].*?(?=\n|$)",
        r"(?i)\[fatal\].*?(?=\n|$)",
        # Python specific errors
        r"(?i)NameError:.*?(?=\n|$)",
        r"(?i)AttributeError:.*?(?=\n|$)",
        r"(?i)ImportError:.*?(?=\n|$)",
        r"(?i)ModuleNotFoundError:.*?(?=\n|$)",
        r"(?i)TypeError:.*?(?=\n|$)",
        r"(?i)ValueError:.*?(?=\n|$)",
        r"(?i)KeyError:.*?(?=\n|$)",
        r"(?i)IndexError:.*?(?=\n|$)",
        r"(?i)RuntimeError:.*?(?=\n|$)",
        # Java exceptions
        r"java\.lang\.\w+Exception.*?(?=\n|$)",
        # Specific patterns
        r"Not able to.*?(?=\n|$)",
        r"Replication job failed.*?(?=\n|$)",
        # Airflow specific
        r"Task failed with exception.*?(?=\n|$)",
        r"Failed to execute job.*?(?=\n|$)",
        r"Marking task as FAILED.*?(?=\n|$)",
    ]

    # Exclusion patterns - add patterns here for errors you want to exclude
    exclusion_patterns = [
        # Exclude pod metadata dictionaries
        r"remote_pod:\s*\{.*?\}(?=\n\n|\Z)",  # Matches remote_pod dictionary
        r"'api_version':\s*'v1'",  # Matches lines containing api_version
        r"'kind':\s*'Pod'",  # Matches lines containing Pod kind
        r"'metadata':\s*\{",  # Matches metadata dictionary starts
        r"'spec':\s*\{",  # Matches spec dictionary starts
        r"'status':\s*\{",  # Matches status dictionary starts
        # Exclude datetime objects in logs
        r"datetime\.datetime\(",
        # Exclude dictionary-like structures
        r"^\s*\{.*\}\s*$",  # Lines that are just dictionaries
        r"^\s*'[^']+'\s*:\s*",  # Lines starting with dictionary keys
        # Exclude log metadata
        r"^\s*INFO\s*-\s*::",  # Info log prefixes
        r"^\s*\[\d{4}-\d{2}-\d{2}",  # Timestamp prefixes
    ]

    # Keywords to exclude - if error contains these words, it will be excluded
    exclusion_keywords = [
        "remote_pod:",
        "'api_version':",
        "'metadata':",
        "'spec':",
        "'status':",
        "datetime.datetime(",
        "'container_statuses':",
        "'volume_mounts':",
        "'managed_fields':",
        "::group::",
        "::endgroup::",
    ]

    errors = []
    for pattern in error_patterns:
        matches = re.findall(pattern, log_content, re.MULTILINE | re.DOTALL)
        errors.extend(matches)

    # Also extract specific error messages from the logs
    # Look for the actual error that caused the failure
    specific_error_pattern = r"(?:NameError|AttributeError|ImportError|ModuleNotFoundError|TypeError|ValueError|KeyError|IndexError|RuntimeError):\s*(.+?)(?:\n|$)"
    specific_matches = re.findall(specific_error_pattern, log_content, re.MULTILINE)
    for match in specific_matches:
        errors.append(f"Python Error: {match}")

    # Filter out excluded patterns
    filtered_errors = []
    for error in errors:
        # Check if error matches any exclusion pattern
        exclude = False

        # Check regex exclusion patterns
        for exclusion_pattern in exclusion_patterns:
            if re.search(exclusion_pattern, error, re.IGNORECASE | re.DOTALL):
                exclude = True
                break

        # Check keyword exclusions
        if not exclude:
            for keyword in exclusion_keywords:
                if keyword in error:
                    exclude = True
                    break

        # Additional check: exclude if it looks like a Python dictionary/object representation
        if not exclude:
            # Check if the error contains multiple dictionary-like patterns
            dict_indicators = [
                "':",
                "': {",
                "': [",
                "datetime.datetime(",
                "None,",
                "True,",
                "False,",
            ]
            indicator_count = sum(
                1 for indicator in dict_indicators if indicator in error
            )
            if (
                indicator_count >= 3
            ):  # If it has 3 or more dictionary indicators, likely a dict dump
                exclude = True

        # Add to filtered list if not excluded
        if not exclude:
            filtered_errors.append(error)

    # Remove duplicates while preserving order
    seen = set()
    unique_errors = []
    for error in filtered_errors:
        # Clean up the error message
        error = error.strip()
        if error and error not in seen:
            seen.add(error)
            unique_errors.append(error)

    # If no errors found but there's a clear failure message, extract it
    if not unique_errors and "Task failed" in log_content:
        # Try to extract the specific failure reason
        failure_pattern = r"Failure caused by (.+?)(?:\n|$)"
        failure_matches = re.findall(failure_pattern, log_content)
        if failure_matches:
            unique_errors.extend(
                [f"Task Failure: {match}" for match in failure_matches]
            )

    return unique_errors
