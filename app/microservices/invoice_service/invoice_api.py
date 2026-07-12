# Invoice API.




from fastapi import FastAPI, HTTPException

from app.common.dapr_client import dapr_publish_event

from app.microservices.invoice_service.invoice_service import submit_invoice

from app.common.logger import log_event




# Start the API.
app = FastAPI()




# Health Check. 
@app.get("/health")
def health_check():
    return {
        "status": "ok",
        "service": "invoice-service",
        "message": "Invoice service is running.",
    }




# POST request: "/invoices".
# If a given payload has no invoice, try to raise a corresponding error. 
# Else try to submit the invoice and log a corresponding message. 
# If the payload has no submission acknowledgement, try to create it.
# Anyway try to save the submission in a Dapr state, and if an error occurs log a corresponding message. 
# Anyway try to publish a corresponding event through a Dapr Pub/Sub and log a corresponding message. 
# Raise another error if occurs and log a corresponding message. 
@app.post("/invoices")
def submit_invoice_endpoint(payload: dict):

    try:
        
        invoice = payload.get("invoice", payload)
        provided_submit_response = payload.get("submitResponse")

        if not isinstance(invoice, dict):
            raise HTTPException(
                status_code = 400,
                detail = {
                    "status": "bad_request",
                    "message": "Request must include an invoice object.",
                },
            )

        submit_response = submit_invoice(
            invoice = invoice,
            submit_response = provided_submit_response,
        )

        tracking_id = submit_response["trackingId"]
        correlation_id = submit_response["correlationId"]

        dapr_publish_event(
            topic = "invoice.submitted",
            data = {
                "invoice": invoice,
                "submitResponse": submit_response,
            },
        )

        log_event(
            service = "invoice-api",
            event = "invoice_submitted_event_published",
            correlation_id = correlation_id,
            tracking_id = tracking_id,
            message = "invoice-service published invoice.submitted event through Dapr Pub/Sub.",
            vendor = invoice.get("vendor"),
            invoiceNumber = invoice.get("invoiceNumber"),
            amount = invoice.get("total"),
            department = invoice.get("department"),
        )

        return {
            "status": "accepted",
            "message": "Invoice was accepted and invoice.submitted event was published.",
            "submitResponse": submit_response,
            "invoice": invoice,
        }

    except HTTPException:
        raise

    except Exception as error:
        
        log_event(
            service = "invoice-api",
            event = "invoice_submission_failed",
            correlation_id = "unknown",
            tracking_id = "unknown",
            level = "ERROR",
            message = "invoice-service failed while submitting invoice.",
            error = str(error),
        )

        raise HTTPException(
            status_code = 500,
            detail = {
                "status": "invoice_service_error",
                "message": "invoice-service failed while submitting invoice.",
                "error": str(error),
            },
        )