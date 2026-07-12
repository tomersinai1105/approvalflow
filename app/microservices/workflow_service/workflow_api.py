# Workflow API.




from fastapi import FastAPI, HTTPException

from app.common.dapr_client import dapr_publish_event
from app.common.logger import log_event

from app.microservices.workflow_service.workflow import (
    run_invoice_workflow,
    resolve_human_review,
)




# Start the API.
app = FastAPI()




# Health Check. 
@app.get("/health")
def health_check():
    return {
        "status": "ok",
        "service": "workflow-service",
        "message": "Workflow service is running.",
    }




# GET request: "/dapr/subscribe".
# Return the Dapr Pub/Sub subscription configuration for this service.
@app.get("/dapr/subscribe")
def dapr_subscribe():
    return [
        {
            "pubsubname": "pubsub",
            "topic": "invoice.submitted",
            "route": "invoice-submitted",
        }
    ]




# POST request: "/invoice-submitted".
# Try to run the workflow for the invoice and acknowledgement submission, if exist, for a given event and publish the result event. 
# Else try to return a corresponding error. 
# Raise another error if occurs.
# Log corresponding messages
@app.post("/invoice-submitted")
async def invoice_submitted_event(event: dict):

    data = event.get("data", event)

    invoice = data.get("invoice")
    submit_response = data.get("submitResponse")

    if not invoice or not submit_response:
        raise HTTPException(
            status_code = 400,
            detail = {
                "status": "bad_event",
                "message": "invoice.submitted event must include invoice and submitResponse.",
                "event": event,
            },
        )

    correlation_id = submit_response.get("correlationId", "unknown")
    tracking_id = submit_response.get("trackingId", correlation_id)

    log_event(
        service = "workflow-api",
        event = "invoice_submitted_event_received",
        correlation_id = correlation_id,
        tracking_id = tracking_id,
        message = "workflow-service received invoice.submitted event from Dapr Pub/Sub.",
        vendor = invoice.get("vendor"),
        invoiceNumber = invoice.get("invoiceNumber"),
        amount = invoice.get("total"),
        department = invoice.get("department"),
    )

    try:
        
        outputs = await run_invoice_workflow(
            invoice = invoice,
            submit_response = submit_response,
        )

        for output in outputs:
            dapr_publish_event(
                topic = "workflow.finished",
                data = {
                    "trackingId": tracking_id,
                    "correlationId": correlation_id,
                    "workflowOutput": output,
                },
            )

        log_event(
            service = "workflow-api",
            event = "workflow_finished_event_published",
            correlation_id = correlation_id,
            tracking_id = tracking_id,
            message = "workflow-service published workflow.finished event through Dapr Pub/Sub.",
            outputsCount = len(outputs),
        )

        return {
            "status": "processed",
            "trackingId": tracking_id,
            "correlationId": correlation_id,
            "outputs": outputs,
        }

    except Exception as error:
        log_event(
            service = "workflow-api",
            event = "workflow_event_processing_failed",
            correlation_id = correlation_id,
            tracking_id = tracking_id,
            level = "ERROR",
            message = "workflow-service failed while processing invoice.submitted event.",
            error = str(error),
        )

        raise HTTPException(
            status_code = 500,
            detail = {
                "status": "workflow_processing_error",
                "message": "workflow-service failed while processing invoice.submitted event.",
                "error": str(error),
            },
        )
        
        
        
        
# POST request: "/run".
# Try to run the workflow for a given payload and return the result. 
# Else try to return a corresponding error. 
# Raise another error if occurs.
@app.post("/run")
async def run_workflow_endpoint(payload: dict):

    invoice = payload.get("invoice")
    submit_response = payload.get("submitResponse")

    if not invoice:
        raise HTTPException(
            status_code = 400,
            detail = {
                "status": "bad_request",
                "message": "invoice is required.",
            },
        )

    try:
        outputs = await run_invoice_workflow(
            invoice = invoice,
            submit_response = submit_response,
        )

        return {
            "status": "processed",
            "outputs": outputs,
        }

    except Exception as error:
        raise HTTPException(
            status_code = 500,
            detail = {
                "status": "workflow_run_error",
                "message": "workflow-service failed while running workflow.",
                "error": str(error),
            },
        )
        
        
        
        
# POST request: "/human-review/{correlation_id}/resolve".
# Resolve human review for a given correlation id based on a given payload, if the payload has action.
# Else return a corresponding error. 
@app.post("/human-review/{correlation_id}/resolve")
def resolve_human_review_endpoint(correlation_id: str, payload: dict):


    action = payload.get("action")
    approver = payload.get("approver", "human.approver@northwind.example")
    comment = payload.get("comment")

    if not action:
        raise HTTPException(
            status_code = 400,
            detail = {
                "status": "bad_request",
                "message": "action is required.",
            },
        )

    result = resolve_human_review(
        correlation_id = correlation_id,
        action = action,
        approver = approver,
        comment = comment,
    )

    return result