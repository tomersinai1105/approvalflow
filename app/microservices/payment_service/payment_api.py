# Payment API.




from fastapi import FastAPI, HTTPException

from app.microservices.payment_service.payment_service import (
    get_budget,
    run_payment_saga,
)

from app.common.logger import log_event




# Start the API.
app = FastAPI()




# Health Check.
@app.get("/health")
def health_check():
    return {
        "status": "ok",
        "service": "payment-service",
        "message": "Payment service is running.",
    }




# POST request: "/payment/saga".
# If a given payload has no correlation ID, raise a corresponding error.
# Else try to run a payment saga based on the payment and log a corresponding message.
# Raise another error if occurs. 
@app.post("/payment/saga")
def payment_saga_endpoint(payload: dict):

    correlation_id = payload.get("correlationId")
    tracking_id = payload.get("trackingId", correlation_id)
    invoice = payload.get("invoice") or {}
    amount_usd = payload.get("amountUsd")

    if not correlation_id:
        raise HTTPException(
            status_code = 400,
            detail = {
                "status": "bad_request",
                "message": "correlationId is required.",
            },
        )

    try:
        result = run_payment_saga(
            correlation_id = correlation_id,
            tracking_id = tracking_id,
            invoice = invoice,
            amount_usd = amount_usd,
        )

        log_event(
            service = "payment-service",
            event = "payment_saga_endpoint_completed",
            correlation_id = correlation_id,
            tracking_id = tracking_id,
            message = "Payment saga endpoint completed.",
            sagaStatus = result.get("status"),
            amountUsd = result.get("amountUsd"),
        )

        return result

    except Exception as error:
        raise HTTPException(
            status_code = 500,
            detail = {
                "status": "payment_saga_error",
                "message": "payment-service failed while running payment saga.",
                "error": str(error),
            },
        )




# GET request: "/budgets/{department}".
# Try to return the budget of a given department.  
# Raise an error if occurs. 
@app.get("/budgets/{department}")
def budget_endpoint(department: str):

    try:
        return get_budget(department)

    except Exception as error:
        raise HTTPException(
            status_code = 500,
            detail = {
                "status": "budget_lookup_error",
                "message": "payment-service failed while reading budget.",
                "error": str(error),
            },
        )