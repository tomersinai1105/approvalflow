# Audit service building an for audit trail.



from app.common.dapr_client import dapr_save_state

from datetime import datetime, UTC



"""
# Return from given invoice and preprocess result a clean section of extracted data for the audit trail.
def build_extracted_data(invoice: dict, preprocess_result: dict | None) -> dict:

    preprocess_result = preprocess_result or {}

    return {
        "submitter": invoice.get("submitter"),
        "department": invoice.get("department"),
        "vendor": invoice.get("vendor"),
        "vendorKnown": invoice.get("vendorKnown"),
        "invoiceNumber": invoice.get("invoiceNumber"),
        "category": invoice.get("category"),
        "currency": invoice.get("currency"),
        "declaredTotal": invoice.get("total"),
        "amountUsd": preprocess_result.get("amountUsd"),
        "date": invoice.get("date"),
        "receiptPresent": invoice.get("receiptPresent"),
        "lineItems": invoice.get("lineItems", []),
    }




# Return from a given preprocess result a clean section of preprocess for the audit trail. 
def build_preprocess_audit(preprocess_result: dict | None) -> dict:

    if preprocess_result is None:
        return {
            "status": "not_available",
            "message": "Preprocess did not run because input validation failed.",
        }

    return {
        "math": {
            "mathValid": preprocess_result.get("mathValid"),
            "lineItemsSum": preprocess_result.get("lineItemsSum"),
            "taxAmount": preprocess_result.get("taxAmount"),
            "calculatedTotal": preprocess_result.get("calculatedTotal"),
            "declaredTotal": preprocess_result.get("declaredTotal"),
        },
        "currency": {
            "currency": preprocess_result.get("currency"),
            "fxRate": preprocess_result.get("fxRate"),
            "amountUsd": preprocess_result.get("amountUsd"),
        },
        "globalSignals": {
            "receiptRequired": preprocess_result.get("receiptRequired"),
            "receiptPresent": preprocess_result.get("receiptPresent"),
            "vendorKnown": preprocess_result.get("vendorKnown"),
            "duplicate": preprocess_result.get("duplicate"),
            "fraudSignal": preprocess_result.get("fraudSignal"),
            "fraudSignals": preprocess_result.get("fraudSignals", []),
            "payloadInstructionSignal": preprocess_result.get("payloadInstructionSignal"),
            "hardStops": preprocess_result.get("hardStops", []),
        },
        "idempotency": {
            "signature": preprocess_result.get("signature"),
        },
    }




# Return from a given classification made by an AI agent a clean section of agent decision for the audit trail.
def build_agent_decision(classification: dict | None) -> dict | None:

    if classification is None:
        return None

    return {
        "recommendation": classification.get("recommendation"),
        "confidence": classification.get("confidence"),
        "category": classification.get("category"),
        "citedRules": classification.get("citedRules", []),
        "policyViolations": classification.get("policyViolations", []),
        "reason": classification.get("reason"),
        "ignoredPayloadInstructions": classification.get("ignoredPayloadInstructions"),
    }




# Return from given router response and final decision maker a clean section of router decision for the audit trail. 
def build_router_decision(router_response: dict, final_decision_made_by: str) -> dict:

    return {
        "finalRoute": router_response.get("finalRoute"),
        "appliedRules": router_response.get("appliedRules", []),
        "reason": router_response.get("reason"),
        "finalDecisionMadeBy": final_decision_made_by,
    }




# Return from a given payment outcome a clean section of payment outcome for the audit trail.
def build_payment_outcome(payment_saga: dict | None) -> dict:

    if payment_saga is None:
        return {
            "status": "not_started",
            "paymentId": None,
            "reservationId": None,
            "budgetStatus": None,
            "compensationStatus": None,
            "reason": "Payment has not started.",
        }

    reservation = payment_saga.get("reservation") or {}
    payment = payment_saga.get("payment") or {}
    budget_commitment = payment_saga.get("budgetCommitment") or {}
    compensation = payment_saga.get("compensation") or {}

    return {
        "status": payment_saga.get("status"),
        "message": payment_saga.get("message"),
        "paymentId": payment.get("paymentId"),
        "paymentReason": payment.get("reason"),
        "reservationId": reservation.get("reservationId"),
        "reservationStatus": reservation.get("status"),
        "department": reservation.get("department"),
        "amountUsd": reservation.get("amountUsd"),
        "budgetBefore": reservation.get("budgetBefore"),
        "budgetAfterReservation": reservation.get("budgetAfter"),
        "budgetCommitmentStatus": budget_commitment.get("status"),
        "compensationStatus": compensation.get("status"),
        "compensationReason": compensation.get("reason"),
    }




# Return a clean section of decision trail based on given details for the audit trail.. 
def build_decision_trail(
    router_response: dict,
    invoice: dict,
    final_decision_made_by: str,
    payment_saga: dict | None = None,
    human_decision: dict | None = None,
) -> dict:

    classification = router_response.get("classification")
    preprocess_result = router_response.get("preprocess")

    return {
        "correlationId": router_response["correlationId"],
        "trackingId": router_response.get("trackingId"),
        "extractedData": build_extracted_data(invoice, preprocess_result),
        "preprocess": build_preprocess_audit(preprocess_result),
        "agentDecision": build_agent_decision(classification),
        "routerDecision": build_router_decision(
            router_response = router_response,
            final_decision_made_by = final_decision_made_by,
        ),
        "humanDecision": human_decision,
        "paymentOutcome": build_payment_outcome(payment_saga),
    }




# Return an audit trail based on given details. 
def build_final_result(
    router_response: dict,
    invoice: dict,
    status: str,
    message: str,
    final_decision_made_by: str,
    payment_saga: dict | None = None,
    human_decision: dict | None = None,
    human_review_task: dict | None = None,
) -> dict:
    
    result = {
        "trackingId": router_response["trackingId"],
        "correlationId": router_response["correlationId"],
        "finalRoute": router_response["finalRoute"],
        "status": status,
        "message": message,
        "decisionTrail": build_decision_trail(
            router_response = router_response,
            invoice = invoice,
            final_decision_made_by = final_decision_made_by,
            payment_saga = payment_saga,
            human_decision = human_decision,
        ),
    }

    if human_review_task is not None:
        result["humanReviewTask"] = human_review_task

    return result
"""




















# =========================
# Helpers
# =========================

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