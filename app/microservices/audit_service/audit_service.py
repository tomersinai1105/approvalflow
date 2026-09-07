# Audit service building an for audit trail.



from app.common.dapr_client import dapr_save_state

from datetime import datetime, UTC




# Return a final result for audit trail based on a given payload. 
def build_final_result_from_payload(payload: dict) -> dict:

    router_response = payload.get("routerResponse") or {}
    
    invoice = payload.get("invoice") or {}
    
    payment_saga = payload.get("paymentSaga")
    
    human_decision = payload.get("humanDecision")
    
    human_review_task = payload.get("humanReviewTask")

    correlation_id = router_response.get(
        "correlationId",
        invoice.get("correlationId", "unknown"),
    )

    tracking_id = router_response.get(
        "trackingId",
        invoice.get("trackingId", correlation_id),
    )

    status = payload.get("status")
    
    message = payload.get("message")
    
    final_decision_made_by = payload.get("finalDecisionMadeBy")

    result = {
        "trackingId": tracking_id,
        "correlationId": correlation_id,
        "invoiceId": invoice.get("id"),
        "invoiceNumber": invoice.get("invoiceNumber"),
        "vendor": invoice.get("vendor"),
        "department": invoice.get("department"),
        "category": invoice.get("category"),
        "amount": invoice.get("total"),
        "currency": invoice.get("currency", "USD"),
        "status": status,
        "message": message,
        "finalRoute": router_response.get("finalRoute"),
        "finalDecisionMadeBy": final_decision_made_by,
        "appliedRules": router_response.get("appliedRules", []),
        "reason": router_response.get("reason"),
        "classification": router_response.get("classification"),
        "preprocess": router_response.get("preprocess"),
        "paymentSaga": payment_saga,
        "humanDecision": human_decision,
        "humanReviewTask": human_review_task,
        "auditTrail": {
            "createdAt": datetime.now(UTC).isoformat(),
            "routerResponse": router_response,
            "paymentOutcome": payment_saga,
            "humanDecision": human_decision,
            "humanReviewTask": human_review_task,
        },
    }

    return result




# Save a given result of audio trail in a Dapr state. 
def save_audit_record(result: dict) -> None:

    correlation_id = result.get("correlationId")
    tracking_id = result.get("trackingId")

    if not correlation_id:
        return

    dapr_save_state(
        key = f"audit:{correlation_id}",
        value = result,
    )

    if tracking_id:
        dapr_save_state(
            key = f"audit-tracking:{tracking_id}",
            value = correlation_id,
        )