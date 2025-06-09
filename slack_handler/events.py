from fastapi import FastAPI, APIRouter, Request, HTTPException
from fastapi.responses import JSONResponse, PlainTextResponse
import json
import uuid
import logging
from datetime import datetime, timezone
from slack_handler.utils import parse_slack_text
from slack_handler.verifier import verify_slack_signature
from agent.graph import graph
from agent.graph2 import graph2

# from agent.graph2 import is_processing_failure

logger = logging.getLogger(__name__)

slack_events = APIRouter()
# Global set to track processed DAGs
processed_dags = set()


@slack_events.post("/events")
async def handle_slack_event(request: Request):
    global is_processing_failure, processed_dags  # Use the global variable to track processing state

    raw_body = await request.body()
    headers = request.headers

    if not verify_slack_signature(headers, raw_body):
        raise HTTPException(status_code=400, detail="Invalid Slack signature")

    try:
        data = json.loads(raw_body)
    except json.JSONDecodeError:
        raise HTTPException(status_code=400, detail="Invalid JSON")

    if data.get("type") == "url_verification":
        return PlainTextResponse(content=data["challenge"])

    event = data.get("event")

    # logger.info("Received Slack Event: %s", json.dumps(event, indent=2))

    if event and event.get("type") == "message":
        user = event.get("user")
        text = event.get("text")
        channel = event.get("channel")
        timestamp = datetime.now(timezone.utc).isoformat()

        parsed_text = parse_slack_text(text)
        message_data = {
            "id": str(uuid.uuid4()),
            "user": user,
            "channel": channel,
            "timestamp": timestamp,
            "text_details": parsed_text,
        }
        # logger.info("Parsed Slack Message: %s", json.dumps(message_data, indent=2))

        dag_name = message_data.get("text_details", {}).get("dag_name")
        dag_run_id = message_data.get("text_details", {}).get("run_id")

        print(f"dag_name: {dag_name}, dag_run_id: {dag_run_id}")
        if dag_name and dag_run_id:
            # # Create a unique key for the DAG and run ID
            # dag_key = (dag_name, dag_run_id)

            # # Check if this DAG has already been processed
            # if dag_key in processed_dags:
            #     logger.info(
            #         "This DAG has already been processed. Ignoring this message."
            #     )
            #     return JSONResponse(
            #         content={"status": "ok", "message": "Already processed."}
            #     )

            # # Mark this DAG as processed
            # processed_dags.add(dag_key)

            # logger.info(
            #     "Incoming Slack Message: %s", json.dumps(message_data, indent=2)
            # )

            # Proceed with processing the DAG
            try:
                is_processing_failure = (
                    True  # Set the flag to indicate processing has started
                )
                response = graph2.invoke({"messages": message_data})
                is_processing_failure = False  # Reset the flag after processing
                return JSONResponse(content={"status": "ok", "response": response})
            except Exception as e:
                logger.exception("Error invoking graph:")
                is_processing_failure = False  # Reset the flag on error
                raise HTTPException(status_code=500, detail=str(e))

        logger.info(
            "Incoming Slack Message (no dag_name or dag_run_id): %s",
            json.dumps(message_data, indent=2),
        )
        return JSONResponse(
            content={"status": "ok", "message": "No DAG name or run ID found."}
        )

    return JSONResponse(content={"status": "ok"})  # Non-message event


# @slack_events.post("/events")
# async def handle_slack_event(request: Request):
#     global processed_dags  # Use the global variable to track processing state

#     raw_body = await request.body()
#     headers = request.headers

#     if not verify_slack_signature(headers, raw_body):
#         raise HTTPException(status_code=400, detail="Invalid Slack signature")

#     try:
#         data = json.loads(raw_body)
#     except json.JSONDecodeError:
#         raise HTTPException(status_code=400, detail="Invalid JSON")

#     if data.get("type") == "url_verification":
#         return PlainTextResponse(content=data["challenge"])

#     event = data.get("event")

#     logger.info("Received Slack Event: %s", json.dumps(event, indent=2))

#     if event and event.get("type") == "message":
#         try:
#             response = graph2.invoke({"messages": event})
#             return JSONResponse(content={"status": "ok", "response": response})
#         except Exception as e:
#             logger.exception("Error invoking graph:")
#             raise HTTPException(status_code=500, detail=str(e))

#     return JSONResponse(content={"status": "ok"})
