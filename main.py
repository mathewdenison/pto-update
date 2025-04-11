import os
import json
import logging
import base64

from google.cloud import pubsub_v1
from google.cloud import logging as cloud_logging

# Import shared utilities from your PyPI package.
from pto_common_timesheet_mfdenison_hopkinsep.utils.pto_update_manager import PTOUpdateManager
from pto_common_timesheet_mfdenison_hopkinsep.utils.dashboard_events import build_dashboard_payload

# Set up Google Cloud Logging
cloud_log_client = cloud_logging.Client()
cloud_log_client.setup_logging()

# Configure standard logging
logger = logging.getLogger("pto_update_worker")
logger.setLevel(logging.INFO)

# Pub/Sub dashboard topic configuration – project id comes from environment variables.
PROJECT_ID = os.environ.get("PROJECT_ID", "hopkinstimesheetproj")
DASHBOARD_TOPIC = f"projects/{PROJECT_ID}/topics/dashboard-queue"
publisher = pubsub_v1.PublisherClient()

def pto_update_processing(event, context):
    """
    Cloud Function to process a PTO update message.

    Expected message payload (JSON):
      {
         "employee_id": ...,
         "new_balance": ...
      }

    The function:
      - Decodes the base64-encoded message.
      - Parses JSON (double-parses if necessary).
      - Processes the PTO update with PTOUpdateManager.
      - Builds a dashboard payload.
      - Publishes the payload to the dashboard Pub/Sub topic.
    """
    try:
        # Decode the Pub/Sub message (base64 encoded).
        raw_data = base64.b64decode(event["data"]).decode("utf-8")
        logger.info(f"Raw message received: {raw_data}")

        # Decode the JSON payload. If the result is a string, decode it again.
        update_data = json.loads(raw_data)
        if isinstance(update_data, str):
            update_data = json.loads(update_data)
        logger.info(f"Message payload: {update_data}")

        # Retrieve required fields.
        employee_id = update_data["employee_id"]
        new_balance = update_data["new_balance"]

        # Process the PTO update.
        update_manager = PTOUpdateManager(employee_id, new_balance)
        result = update_manager.update_pto()

        # Build the appropriate dashboard payload.
        if result["result"] == "success":
            log_msg = f"[SUCCESS] PTO for employee_id {employee_id} updated to {new_balance}"
            logger.info(log_msg)
            dashboard_payload = build_dashboard_payload(
                employee_id,
                "refresh_data",
                "Time log created, please refresh dashboard data."
            )
        else:
            log_msg = f"[ERROR] Failed to update PTO for employee_id {employee_id}. Reason: {result['message']}"
            logger.error(log_msg)
            dashboard_payload = build_dashboard_payload(
                employee_id,
                "pto_updated",
                log_msg
            )

        # Publish the dashboard payload.
        future = publisher.publish(DASHBOARD_TOPIC, json.dumps(dashboard_payload).encode("utf-8"))
        future.result()  # Optionally wait for the publish to finish.
        logger.info("Published update to dashboard Pub/Sub topic.")

    except Exception as e:
        logger.exception(f"Error processing message: {str(e)}")
        # Raising the exception will cause the function to signal failure (and potentially trigger a retry).
        raise
