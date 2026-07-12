# Audit API. 




from fastapi import FastAPI, HTTPException


from app.common.dapr_client import (
    dapr_get_state,
    DaprClientError,
)
from app.microservices.audit_service.audit_service import ( 
    build_final_result_from_payload, 
    save_audit_record,
)


from app.common.logger import log_event




# Start the API.
app = FastAPI()




# Health Check. 
@app.get("/health")
def health_check():
    return {
        "status": "ok",
        "service": "audit-service",
        "message": "Audit service is running.",
    }




# POST request: "/final-result".
# Try to save in a Dapr state and return a final result for audit trail based on a given payload. 
# Try to log a corresponding message.
# Raise an error if occurs. 
@app.post("/final-result")
def final_result_endpoint(payload: dict):

    try:
        
        result = build_final_result_from_payload(payload)
        save_audit_record(result)

        log_event(
            service = "audit-service",
            event = "final_result_created",
            correlation_id = result.get("correlationId", "unknown"),
            tracking_id = result.get("trackingId", "unknown"),
            message = "Final audit result was created and saved.",
            status = result.get("status"),
            finalRoute = result.get("finalRoute"),
            finalDecisionMadeBy = result.get("finalDecisionMadeBy"),
        )

        return {
            "result": result,
        }

    except Exception as error:
        raise HTTPException(
            status_code = 500,
            detail = {
                "status": "audit_final_result_error",
                "message": "audit-service failed while building final result.",
                "error": str(error),
            },
        )




# GET request: "/audit/by-tracking/{tracking_id}".
# Try to return the audit trail for a given tracking ID.  
# Raise an error if occurs. 
@app.get("/audit/by-tracking/{tracking_id}")
def get_audit_by_tracking_id(tracking_id: str):

    try:
        correlation_id = dapr_get_state(
            key=f"audit-tracking:{tracking_id}",
            default=None,
        )

        if correlation_id is None:
            return {
                "status": "not_found",
                "trackingId": tracking_id,
                "message": "No audit record found for this tracking id.",
            }

        result = dapr_get_state(
            key = f"audit:{correlation_id}",
            default = None,
        )

        if result is None:
            return {
                "status": "not_found",
                "trackingId": tracking_id,
                "correlationId": correlation_id,
                "message": "Audit index exists, but audit record was not found.",
            }

        return result

    except DaprClientError as error:
        raise HTTPException(
            status_code = 500,
            detail = {
                "status": "audit_lookup_error",
                "message": "audit-service failed while reading audit record.",
                "error": str(error),
            },
        )




# GET request: "/audit/{correlation_id}".
# Try to return the audit trail for a given correlation ID.  
# Raise an error if occurs. 
@app.get("/audit/{correlation_id}")
def get_audit_by_correlation_id(correlation_id: str):

    try:
        result = dapr_get_state(
            key = f"audit:{correlation_id}",
            default = None,
        )

        if result is None:
            return {
                "status": "not_found",
                "correlationId": correlation_id,
                "message": "No audit record found for this correlation id.",
            }

        return result

    except DaprClientError as error:
        raise HTTPException(
            status_code = 500,
            detail = {
                "status": "audit_lookup_error",
                "message": "audit-service failed while reading audit record.",
                "error": str(error),
            },
        )
