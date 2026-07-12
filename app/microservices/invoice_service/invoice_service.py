# Invoice service for submitting invoices and getting immediate submission acknowledgements.




from uuid import uuid4
from datetime import datetime, UTC

from app.common.dapr_client import dapr_save_state, DaprClientError

from app.common.logger import log_event




# Submit an initial submission and return an immediate submission acknowledgement.
def create_submit_response() -> dict:

    tracking_id = f"INV-{uuid4().hex[:8].upper()}"
    correlation_id = tracking_id

    return {
        "trackingId": tracking_id,
        "correlationId": correlation_id,
        "status": "accepted",
        "receivedAt": datetime.now(UTC).isoformat(),
        "message": "Invoice accepted for asynchronous processing.",
    }




# Submit a given invoice and log a corresponding message. 
# If a given submission acknowledgement doesn't exists, create it, and anyway return it.
# Try to save the submission in a Dapr state, and if an error occurs log a corresponding message. 
def submit_invoice(
    invoice: dict | None = None,
    submit_response: dict | None = None,
) -> dict:

    if submit_response is None:
        submit_response = create_submit_response()

    tracking_id = submit_response["trackingId"]
    correlation_id = submit_response["correlationId"]

    submission_record = {
        "submit": submit_response,
        "invoice": invoice,
        "workflowStatus": "accepted",
        "workflowFinished": False,
        "workflowOutputs": [],
        "error": None,
    }

    try:
        dapr_save_state(
            key = f"invoice-submission:{tracking_id}",
            value = submission_record,
        )

    except DaprClientError as error:
        log_event(
            service = "invoice-service",
            event = "invoice_submission_state_save_failed",
            correlation_id = correlation_id,
            tracking_id = tracking_id,
            level = "WARNING",
            message = "invoice-service could not save submission state to Dapr.",
            error = str(error),
        )

    log_event(
        service = "invoice-service",
        event = "invoice_submission_created",
        correlation_id = correlation_id,
        tracking_id = tracking_id,
        message = "Invoice submission acknowledgement was created.",
        vendor = (invoice or {}).get("vendor"),
        invoiceNumber = (invoice or {}).get("invoiceNumber"),
        amount = (invoice or {}).get("total"),
        department = (invoice or {}).get("department"),
    )

    return submit_response