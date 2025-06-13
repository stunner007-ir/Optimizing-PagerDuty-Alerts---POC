import time
import hmac
import hashlib

# from config import SLACK_SIGNING_SECRET
from dotenv import load_dotenv
import os

load_dotenv()


SLACK_SIGNING_SECRET = os.getenv("SLACK_SIGNING_SECRET")


def verify_slack_signature(headers, raw_body: bytes) -> bool:
    slack_signature = headers.get("X-Slack-Signature")
    slack_timestamp = headers.get("X-Slack-Request-Timestamp")

    if not slack_signature or not slack_timestamp:
        return False

    if abs(time.time() - float(slack_timestamp)) > 300:
        return False

    sig_basestring = f"v0:{slack_timestamp}:{raw_body.decode()}"
    my_signature = (
        "v0="
        + hmac.new(
            SLACK_SIGNING_SECRET.encode(), sig_basestring.encode(), hashlib.sha256
        ).hexdigest()
    )
    return hmac.compare_digest(my_signature, slack_signature)
