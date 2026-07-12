# API Gateway. 




from fastapi import FastAPI, Header, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, StreamingResponse

import asyncio
import hashlib
import json
import time
from collections import defaultdict, deque

from app.common.dapr_client import (
    dapr_get_state,
    dapr_invoke_service,
    dapr_save_state,
    DaprClientError,
)

from app.common.logger import log_event




KNOWN_VENDORS = [
    "Bistro 19",
    "Atlassian",
    "Dell",
    "Office Depot",
    "The Rooftop Grill",
    "Trattoria Verde",
    "Hotel Adler",
    "RackSpace Supplies",
    "PixelForge",
]


RATE_LIMIT_WINDOW_SECONDS = 60
RATE_LIMIT_MAX_REQUESTS = 120


REQUEST_LOG = defaultdict(deque)

NOTIFICATION_CHANNELS = {}




# Start the API.
app = FastAPI()


# start CORS. 
app.add_middleware(
    CORSMiddleware,
    allow_origins = ["*"],
    allow_credentials = True,
    allow_methods = ["*"],
    allow_headers = ["*"],
)




# Minimal API gateway rate limiting.
@app.middleware("http")
async def rate_limit_middleware(request: Request, call_next):

    client_host = request.client.host if request.client else "unknown"
    
    now = time.time()

    requests = REQUEST_LOG[client_host]

    while requests and requests[0] < now - RATE_LIMIT_WINDOW_SECONDS:
        requests.popleft()

    if len(requests) >= RATE_LIMIT_MAX_REQUESTS:
        return JSONResponse(
            status_code = 429,
            content = {
                "status": "rate_limited",
                "message": "Too many requests. Please try again later.",
            },
        )

    requests.append(now)

    return await call_next(request)




# Dapr state keys.

def gateway_submission_key(tracking_id: str) -> str:
    return f"gateway-submission:{tracking_id}"

def gateway_status_key(tracking_id: str) -> str:
    return f"gateway-status:{tracking_id}"

def idempotency_key_state_key(idempotency_key: str) -> str:
    return f"gateway-idempotency:{idempotency_key}"




# Return whether a known vendor is known. 
def is_known_vendor(vendor: str | None) -> bool:
    
    if not vendor:
        return False

    normalized_vendor = vendor.strip().lower()

    is_known_vendor = False
    
    for known_vendor in KNOWN_VENDORS:
        if normalized_vendor == known_vendor.lower():
            is_known_vendor = True
            break
    
    return is_known_vendor




# Return a unique stable fingerprint for a given invoice data. 
def build_invoice_fingerprint(invoice_data: dict) -> str:
    
    canonical_invoice = json.dumps(
        invoice_data,
        sort_keys = True,
        separators = (",", ":"),
        ensure_ascii = False,
    )

    return hashlib.sha256(canonical_invoice.encode("utf-8")).hexdigest()




# Save submission and status of given tracking ID and value in the Dapr state store.

def save_gateway_submission(tracking_id: str, value: dict) -> None:
    dapr_save_state(
        key = gateway_submission_key(tracking_id),
        value = value,
    )

def save_gateway_status(tracking_id: str, value: dict) -> None:
    dapr_save_state(
        key = gateway_status_key(tracking_id),
        value = value,
    )


# Return submission and state of a given tracking ID from the Dapr state store. 

def get_gateway_submission(tracking_id: str) -> dict | None:
    return dapr_get_state(
        key = gateway_submission_key(tracking_id),
        default = None,
    )

def get_gateway_status(tracking_id: str) -> dict | None:
    return dapr_get_state(
        key = gateway_status_key(tracking_id),
        default = None,
    )




# Put a given payload in a notification channel, if exists, of a given tracking ID.
async def publish_final_notification(tracking_id: str, payload: dict) -> None:
    
    queue = NOTIFICATION_CHANNELS.get(tracking_id)
    
    if queue is not None:
        await queue.put(payload)




# Health Check. 
@app.get("/health")
def health_check():
    
    log_event(
        service = "api-gateway",
        event = "health_check_requested",
        correlation_id = "system",
        tracking_id = "system",
        message = "Health check was requested.",
    )

    return {
        "status": "ok",
        "service": "api-gateway",
        "message": "API Gateway is running.",
    }


# Readiness Check. 
@app.get("/ready")
def readiness_check():

    return {
        "status": "ready",
        "service": "api-gateway",
        "message": "API Gateway is ready.",
    }




# GET request: "/dapr/subscribe".
# Return the Dapr Pub/Sub subscription configuration for this service.
@app.get("/dapr/subscribe")
def dapr_subscribe():

    return [
        {
            "pubsubname": "pubsub",
            "topic": "workflow.finished",
            "route": "workflow-finished",
        }
    ]




# POST request: "/workflow-finished".
# If a given event has no tracking ID, return a corresponding error.
# Else save the event in the Dapr state store, save in the notification channel if exists, log a corresponding message, and return a corresponding result.
@app.post("/workflow-finished")
async def workflow_finished_event(event: dict):


    data = event.get("data", event)

    tracking_id = data.get("trackingId")
    correlation_id = data.get("correlationId")
    workflow_output = data.get("workflowOutput") or {}

    if not tracking_id:
        raise HTTPException(
            status_code = 400,
            detail = {
                "status": "bad_event",
                "message": "workflow.finished event must include trackingId.",
                "event": event,
            },
        )

    status_record = {
        "trackingId": tracking_id,
        "correlationId": correlation_id,
        "workflowStatus": "finished",
        "workflowFinished": True,
        "status": workflow_output.get("status", "finished"),
        "finalRoute": workflow_output.get("finalRoute"),
        "message": workflow_output.get("message"),
        "workflowOutputs": [workflow_output],
        "error": None,
        "result": workflow_output,
    }

    save_gateway_status(
        tracking_id = tracking_id,
        value = status_record,
    )

    await publish_final_notification(
        tracking_id=tracking_id,
        payload = {
            "type": "workflow_finished",
            **status_record,
        },
    )

    log_event(
        service = "api-gateway",
        event = "workflow_finished_event_received",
        correlation_id = correlation_id or tracking_id,
        tracking_id = tracking_id,
        message = "Gateway received workflow.finished event and saved status.",
        status = status_record.get("status"),
        finalRoute = status_record.get("finalRoute"),
    )

    return {
        "status": "SUCCESS",
        "trackingId": tracking_id,
        "correlationId": correlation_id,
    }




# POST request: "/invoices".
# If a given payload has no invoice, return a corresponding error.
# Else try to return an immediate submission acknowledgement, while handling a given idempotency key if exists, and while considering a given default value.
# Raise an error if occurs.
# Anyway log corresponding messages.
@app.post("/invoices")
def submit_invoice_endpoint(
    payload: dict,
    x_idempotency_key: str | None = Header(
        default = None,
        alias = "X-Idempotency-Key",
    ),
):

    invoice_data = payload.get("invoice", payload)

    if not isinstance(invoice_data, dict):
        raise HTTPException(
            status_code = 400,
            detail = {
                "status": "bad_request",
                "message": "Request body must be an invoice object or {'invoice': {...}}.",
            },
        )

    invoice_data = dict(invoice_data)
    invoice_data["vendorKnown"] = is_known_vendor(invoice_data.get("vendor"))

    idempotency_key = None

    if x_idempotency_key is not None and x_idempotency_key.strip() != "":
        idempotency_key = x_idempotency_key.strip()

    invoice_fingerprint = build_invoice_fingerprint(invoice_data)

    try:
        
        if idempotency_key is not None:
            
            existing_record = dapr_get_state(
                key = idempotency_key_state_key(idempotency_key),
                default = None,
            )

            if existing_record is not None:
                
                if existing_record.get("invoiceFingerprint") != invoice_fingerprint:
                    raise HTTPException(
                        status_code = 409,
                        detail = {
                            "status": "idempotency_conflict",
                            "message": (
                                "Idempotency key was already used for a different invoice. "
                                "Use a new X-Idempotency-Key for a new submission."
                            ),
                        },
                    )

                tracking_id = existing_record["trackingId"]
                existing_status = get_gateway_status(tracking_id)

                return {
                    "status": "accepted",
                    "message": "Duplicate retry detected. Returning the original tracking id.",
                    "trackingId": tracking_id,
                    "correlationId": existing_record.get("correlationId", tracking_id),
                    "idempotency": {
                        "replayed": True,
                        "key": idempotency_key,
                    },
                    "currentStatus": existing_status,
                }

        invoice_service_response = dapr_invoke_service(
            app_id = "invoice-service",
            method_name = "invoices",
            payload = {
                "invoice": invoice_data,
            },
            http_method="POST",
        )

        submit_response = invoice_service_response.get("submitResponse") or {}

        tracking_id = submit_response.get("trackingId")
        correlation_id = submit_response.get("correlationId", tracking_id)

        if not tracking_id:
            raise RuntimeError(
                "invoice-service did not return submitResponse.trackingId."
            )

        submission_record = {
            "submit": submit_response,
            "invoice": invoice_data,
            "invoiceServiceResponse": invoice_service_response,
            "workflowStatus": "accepted",
            "workflowFinished": False,
            "workflowOutputs": [],
            "error": None,
        }

        status_record = {
            "trackingId": tracking_id,
            "correlationId": correlation_id,
            "submit": submit_response,
            "invoice": invoice_data,
            "workflowStatus": "accepted",
            "workflowFinished": False,
            "workflowOutputs": [],
            "status": "accepted",
            "finalRoute": None,
            "message": "Invoice accepted for asynchronous processing.",
            "error": None,
            "result": None,
        }

        save_gateway_submission(
            tracking_id = tracking_id,
            value = submission_record,
        )

        save_gateway_status(
            tracking_id = tracking_id,
            value = status_record,
        )

        if idempotency_key is not None:
            dapr_save_state(
                key = idempotency_key_state_key(idempotency_key),
                value = {
                    "key": idempotency_key,
                    "trackingId": tracking_id,
                    "correlationId": correlation_id,
                    "invoiceFingerprint": invoice_fingerprint,
                },
            )

        log_event(
            service="api-gateway",
            event = "invoice_submit_accepted",
            correlation_id = correlation_id,
            tracking_id = tracking_id,
            message = "Gateway invoked invoice-service through Dapr.",
            vendor = invoice_data.get("vendor"),
            invoiceNumber = invoice_data.get("invoiceNumber"),
            amount = invoice_data.get("total"),
            currency = invoice_data.get("currency"),
            department = invoice_data.get("department"),
            idempotencyKeyProvided = idempotency_key is not None,
        )

        return {
            "status": "accepted",
            "message": "Invoice accepted for processing.",
            "trackingId": tracking_id,
            "correlationId": correlation_id,
            "idempotency": {
                "replayed": False,
                "key": idempotency_key,
            },
        }

    except HTTPException:
        raise

    except DaprClientError as error:
        
        log_event(
            service = "api-gateway",
            event = "invoice_submit_dapr_error",
            correlation_id = "unknown",
            tracking_id = "unknown",
            level = "ERROR",
            message = "Gateway failed to invoke invoice-service through Dapr.",
            error = str(error),
        )

        raise HTTPException(
            status_code = 502,
            detail = {
                "status": "dapr_invocation_error",
                "message": "Gateway failed to invoke invoice-service through Dapr.",
                "error": str(error),
            },
        )

    except Exception as error:
        
        log_event(
            service = "api-gateway",
            event = "invoice_submit_error",
            correlation_id = "unknown",
            tracking_id = "unknown",
            level = "ERROR",
            message = "Gateway failed while submitting invoice.",
            error = str(error),
        )

        raise HTTPException(
            status_code = 500,
            detail = {
                "status": "gateway_submit_error",
                "message": "Gateway failed while submitting invoice.",
                "error": str(error),
            },
        )




# GET request: "/invoices/{tracking_id}".
# Try to return thr latest known workflow status of the invoice with a given tracking ID.
# Raise an error if occurs.
# Anyway log corresponding messages.
@app.get("/invoices/{tracking_id}")
def get_invoice_status(tracking_id: str):
    
    try:
        
        status = get_gateway_status(tracking_id)

        if status is None:
            
            log_event(
                service = "api-gateway",
                event = "invoice_status_not_found",
                correlation_id = tracking_id,
                tracking_id = tracking_id,
                level = "ERROR",
                message = "Invoice status was requested but tracking id was not found.",
            )

            return {
                "trackingId": tracking_id,
                "status": "not_found",
                "message": "Invoice not found.",
            }

        log_event(
            service = "api-gateway",
            event = "invoice_status_requested",
            correlation_id = status.get("correlationId", tracking_id),
            tracking_id = tracking_id,
            message = "Invoice status was requested.",
            workflowStatus = status.get("workflowStatus"),
            workflowFinished = status.get("workflowFinished"),
        )

        return status

    except DaprClientError as error:
        raise HTTPException(
            status_code = 500,
            detail = {
                "status": "status_lookup_error",
                "message": "Gateway failed while reading status from Dapr state.",
                "error": str(error),
            },
        )


# GET request: "/invoices/{tracking_id}".
# Return thr latest known workflow status of the invoice with a given tracking ID.
@app.get("/status/{tracking_id}")
def get_status_alias(tracking_id: str):
    return get_invoice_status(tracking_id)


# GET request: "/invoices/{tracking_id}/status".
# Return thr latest known workflow status of the invoice with a given tracking ID.
@app.get("/invoices/{tracking_id}/status")
def get_invoice_status_alias(tracking_id: str):
    return get_invoice_status(tracking_id)




# GET request: "/invoices/{tracking_id}/events".
# Open a notification channel to send events to a given tracking ID.
# Log corresponding messages.
@app.get("/invoices/{tracking_id}/events")
async def invoice_events(tracking_id: str):

    log_event(
        service = "api-gateway",
        event = "notification_channel_opened",
        correlation_id = tracking_id,
        tracking_id = tracking_id,
        message = "Client opened SSE notification channel.",
    )

    async def event_stream():
        
        status = get_gateway_status(tracking_id)

        if status is None:
            
            payload = {
                "type": "invoice_not_found",
                "trackingId": tracking_id,
                "workflowStatus": "not_found",
                "workflowFinished": True,
                "workflowOutputs": [],
                "error": "Invoice not found.",
            }

            yield f"data: {json.dumps(payload, ensure_ascii=False)}\n\n"
            return

        if status.get("workflowFinished") is True:
            payload = {
                "type": "workflow_finished",
                **status,
            }

            yield f"data: {json.dumps(payload, ensure_ascii=False)}\n\n"
            return

        queue = asyncio.Queue()
        NOTIFICATION_CHANNELS[tracking_id] = queue

        try:
            payload = await queue.get()
            yield f"data: {json.dumps(payload, ensure_ascii=False)}\n\n"

        finally:
            
            NOTIFICATION_CHANNELS.pop(tracking_id, None)

            log_event(
                service = "api-gateway",
                event = "notification_channel_closed",
                correlation_id = tracking_id,
                tracking_id = tracking_id,
                message = "SSE notification channel was closed.",
            )

    return StreamingResponse(
        event_stream(),
        media_type="text/event-stream",
    )




# GET request: "/approver/queue"".
# Try to return a queue of all invoices waiting for human review and log corresponding messages.
# Raise error if occurs.
@app.get("/approver/queue")
def get_approver_queue():

    try:
        
        result = dapr_invoke_service(
            app_id = "query-service",
            method_name = "human-review",
            payload = None,
            http_method = "GET",
        )

        items = result.get("items", result)

        log_event(
            service = "api-gateway",
            event = "approver_queue_requested",
            correlation_id = "system",
            tracking_id = "system",
            message = "Approver queue was requested through query-service.",
            queueSize = len(items) if isinstance(items, list) else None,
        )

        return result

    except DaprClientError as error:
        raise HTTPException(
            status_code = 502,
            detail = {
                "status": "query_service_error",
                "message": "Gateway failed to invoke query-service human-review endpoint through Dapr.",
                "error": str(error),
            },
        )


# GET request: "/human-review".
# Try to return a queue of all invoices waiting for human review and log corresponding messages.
# Raise error if occurs.
@app.get("/human-review")
def get_human_review_alias():
    return get_approver_queue()




# POST request: "/approver/queue/{correlation_id}/decision".
# If a given payload has no action, raise a corresponding error.
# Else try to resolve a human review task, based on the payload and a given correlation ID, and log a corresponding message.
# Raise error if occurs.
@app.post("/approver/queue/{correlation_id}/decision")
def resolve_human_review_endpoint(correlation_id: str, payload: dict):
    """
    Resolve a human review task.

    Gateway calls workflow-service through Dapr.
    """

    action = payload.get("action")

    if not action:
        raise HTTPException(
            status_code = 400,
            detail = {
                "status": "bad_request",
                "message": "action is required.",
            },
        )

    try:
        
        result = dapr_invoke_service(
            app_id = "workflow-service",
            method_name = f"human-review/{correlation_id}/resolve",
            payload = payload,
            http_method = "POST",
        )

        tracking_id = result.get("trackingId", correlation_id)

        status_record = {
            "trackingId": tracking_id,
            "correlationId": correlation_id,
            "workflowStatus": "finished",
            "workflowFinished": True,
            "status": result.get("status"),
            "finalRoute": result.get("finalRoute"),
            "message": result.get("message"),
            "workflowOutputs": [result],
            "error": (
                result.get("message")
                if result.get("status") in [
                    "not_found",
                    "invalid_action",
                    "already_resolved",
                    "resume_failed",
                ]
                else None
            ),
            "result": result,
        }

        save_gateway_status(
            tracking_id = tracking_id,
            value = status_record,
        )

        log_event(
            service = "api-gateway",
            event = "human_review_decision_submitted",
            correlation_id = correlation_id,
            tracking_id = tracking_id,
            message = "Human review decision was submitted through workflow-service.",
            action = action,
            approver = payload.get("approver"),
            resultStatus = result.get("status"),
        )

        return result

    except DaprClientError as error:
        raise HTTPException(
            status_code = 502,
            detail = {
                "status": "workflow_service_error",
                "message": "Gateway failed to invoke workflow-service human-review resolve endpoint through Dapr.",
                "error": str(error),
            },
        )


# POST request: "/human-review/{correlation_id}/resolve".
# If a given payload has no action, raise a corresponding error.
# Else try to resolve a human review task, based on the payload and a given correlation ID, and log a corresponding message.
# Raise error if occurs.
@app.post("/human-review/{correlation_id}/resolve")
def resolve_human_review_alias(correlation_id: str, payload: dict):
    return resolve_human_review_endpoint(correlation_id, payload)




# GET request: "/controller/policy-config".
# If a given payload has no action, raise a corresponding error.
# Else try to return the current runtime policy configuration, and log a corresponding message.
# Raise error if occurs.
@app.get("/controller/policy-config")
def get_controller_policy_config():

    try:
        
        config = dapr_invoke_service(
            app_id = "decision-service",
            method_name = "config",
            payload = None,
            http_method = "GET",
        )

        log_event(
            service = "api-gateway",
            event = "controller_policy_config_requested",
            correlation_id = "system",
            tracking_id = "system",
            message = "Controller policy config was requested through decision-service.",
            config = config.get("config", config),
        )

        return config

    except DaprClientError as error:
        raise HTTPException(
            status_code = 502,
            detail = {
                "status": "decision_service_error",
                "message": "Gateway failed to read runtime policy config from decision-service.",
                "error": str(error),
            },
        )




# PUT request: "/controller/policy-config".
# Try to update and return the updated runtime policy configuration, based on a given payload, and log a corresponding message.
# Raise error if occurs.
@app.put("/controller/policy-config")
def update_controller_policy_config(payload: dict):

    try:
        result = dapr_invoke_service(
            app_id = "decision-service",
            method_name = "config",
            payload = payload,
            http_method = "PUT",
        )

        log_event(
            service = "api-gateway",
            event = "controller_policy_config_updated",
            correlation_id = "system",
            tracking_id = "system",
            message = "Controller policy config was updated through decision-service.",
            config = result.get("config", result),
        )

        return result

    except DaprClientError as error:
        raise HTTPException(
            status_code = 502,
            detail = {
                "status": "decision_service_error",
                "message": "Gateway failed to update runtime policy config through decision-service.",
                "error": str(error),
            },
        )


# GET request: "/policy-config".
# If a given payload has no action, raise a corresponding error.
# Else try to return the current runtime policy configuration, and log a corresponding message.
# Raise error if occurs.
@app.get("/policy-config")
def get_policy_config_alias():
    return get_controller_policy_config()


# PUT request: "/policy-config".
# Try to update and return the updated runtime policy configuration, based on a given payload, and log a corresponding message.
# Raise error if occurs.
@app.put("/policy-config")
def update_policy_config_alias(payload: dict):
    return update_controller_policy_config(payload)




# GET request: "/controller/dashboard".
# Try to return a dashboard and log a corresponding message.
# Raise error if occurs.
@app.get("/controller/dashboard")
def get_dashboard():
    
    try:
        dashboard = dapr_invoke_service(
            app_id = "query-service",
            method_name = "dashboard",
            payload = None,
            http_method = "GET",
        )

        log_event(
            service = "api-gateway",
            event ="dashboard_requested",
            correlation_id = "system",
            tracking_id = "system",
            message = "Controller dashboard was requested through query-service.",
            totalItems = dashboard.get("totalItems"),
            autoApprovedCount = dashboard.get("autoApprovedCount"),
            humanReviewCount = dashboard.get("humanReviewCount"),
        )

        return dashboard

    except DaprClientError as error:
        raise HTTPException(
            status_code = 502,
            detail = {
                "status": "query_service_error",
                "message": "Gateway failed to invoke query-service dashboard endpoint through Dapr.",
                "error": str(error),
            },
        )


# GET request: "/dashboard".
# Try to return a dashboard and log a corresponding message.
# Raise error if occurs.
@app.get("/dashboard")
def get_dashboard_alias():
    return get_dashboard()















# GET request: "/audit/{correlation_id}".
# Try to return the full audit trail of a given correlation ID.
# Raise error if occurs. 
@app.get("/audit/{correlation_id}")
def audit_by_correlation_id(correlation_id: str):

    try:
        return dapr_invoke_service(
            app_id = "audit-service",
            method_name = f"audit/{correlation_id}",
            payload = None,
            http_method = "GET",
        )

    except DaprClientError as error:
        raise HTTPException(
            status_code = 502,
            detail = {
                "status": "audit_service_error",
                "message": "Gateway failed to invoke audit-service through Dapr.",
                "error": str(error),
            },
        )


# GET request: "/audit/by-tracking/{tracking_id}".
# Try to return the full audit trail of a given tracking ID.
# Raise error if occurs. 
@app.get("/audit/by-tracking/{tracking_id}")
def audit_by_tracking_id(tracking_id: str):

    try:
        return dapr_invoke_service(
            app_id = "audit-service",
            method_name = f"audit/by-tracking/{tracking_id}",
            payload = None,
            http_method = "GET",
        )

    except DaprClientError as error:
        raise HTTPException(
            status_code = 502,
            detail = {
                "status": "audit_service_error",
                "message": "Gateway failed to invoke audit-service through Dapr.",
                "error": str(error),
            },
        )