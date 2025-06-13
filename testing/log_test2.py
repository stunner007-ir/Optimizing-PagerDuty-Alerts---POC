import re
import os


def extract_errors_from_log(log_content: dict) -> list:
    """Extract error messages from log content"""
    error_patterns = [
        # General error patterns
        r"(?i)error[:\s].*?(?=\n|$)",
        r"(?i)exception[:\s].*?(?=\n|$)",
        r"(?i)failed[:\s].*?(?=\n|$)",
        r"(?i)traceback.*?(?=\n\n|\Z)",
        r"(?i)\[error\].*?(?=\n|$)",
        r"(?i)\[fatal\].*?(?=\n|$)",
        # Java exceptions
        r"java\.lang\.\w+Exception.*?(?=\n|$)",
        # Specific patterns
        r"Not able to.*?(?=\n|$)",
        r"Replication job failed.*?(?=\n|$)",
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
    ]

    errors = []
    for pattern in error_patterns:
        matches = re.findall(pattern, log_content, re.MULTILINE | re.DOTALL)
        errors.extend(matches)

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
        if error not in seen:
            seen.add(error)
            unique_errors.append(error)

    return unique_errors


if __name__ == "__main__":
    log_file = "sample_log.log"  # Change this to your log file name

    with open(log_file, "r") as f:
        original_log = f.read()

    # Extract errors from the original log
    errors = extract_errors_from_log(original_log)

    # Save errors to a file
    output_dir = "extracted_logs.txt"
    with open(output_dir, "w") as f:
        if errors:
            for i, error in enumerate(errors, 1):
                f.write(f"Error {i}:\n{error}\n\n")
        else:
            f.write("No errors found in the log.")
