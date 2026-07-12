# Decision API.




from fastapi import FastAPI, HTTPException

from app.common.dapr_client import (
    dapr_save_state,
    dapr_get_state,
    DaprClientError,
)

from app.microservices.decision_service.decision_service import (
    RUNTIME_CONFIG_STATE_KEY,
    PROCESSED_INVOICE_SIGNATURES,
    validate_invoice_input,
    check_global_rules,
    detect_payload_instructions,
    policy_violation_rule_ids,
    load_runtime_config,
    normalize_runtime_config
)

from app.common.logger import log_event




# Start the API.
app = FastAPI()




# Health Check. 
@app.get("/health")
def health_check():
    return {
        "status": "ok",
        "service": "decision-service",
        "message": "Decision service is running.",
    }




# GET request: "/config".
# Try to return the current runtime configuration from the Dapr state store.
# Raise an error if occurs.
@app.get("/config")
def get_config():
    config = load_runtime_config()
    return {
        "config": config,
    }




# PUT request: "/config".
# Try to update the runtime policy configuration to a given configuration update.
# Try to log a corresponding message.
# Raise an error if occurs. 
@app.put("/config")
def update_config(config_update: dict):

    try:
        
        current_config = load_runtime_config()
        merged_config = {
            **current_config,
            **config_update,
        }

        normalized_config = normalize_runtime_config(merged_config)

        dapr_save_state(
            key = RUNTIME_CONFIG_STATE_KEY,
            value = normalized_config,
        )

        log_event(
            service = "decision-service",
            event = "runtime_config_updated",
            correlation_id = "system",
            tracking_id = "system",
            message = "Runtime policy config was updated.",
            config = normalized_config,
        )

        return {
            "status": "updated",
            "config": normalized_config,
        }

    except ValueError as error:
        raise HTTPException(
            status_code = 400,
            detail = {
                "status": "bad_config",
                "message": str(error),
            },
        )

    except Exception as error:
        raise HTTPException(
            status_code = 500,
            detail = {
                "status": "config_update_error",
                "message": "decision-service failed while updating runtime config.",
                "error": str(error),
            },
        )




# POST request: "/validate".
# Try to validate a given invoice. 
# Try to log a corresponding message.
# Raise an error if occurs. 
@app.post("/validate")
def validate_invoice_endpoint(invoice: dict):

    try:
        
        validation = validate_invoice_input(invoice)

        log_event(
            service = "decision-service",
            event = "invoice_validation_completed",
            correlation_id = invoice.get("correlationId", "unknown"),
            tracking_id = invoice.get("trackingId", "unknown"),
            message = "Invoice validation completed.",
            valid = validation.get("valid"),
            errors = validation.get("errors", []),
        )

        return {
            "validation": validation,
        }

    except Exception as error:
        raise HTTPException(
            status_code = 500,
            detail = {
                "status": "decision_validation_error",
                "message": "decision-service failed during invoice validation.",
                "error": str(error),
            },
        )
        



# POST request: "/policy-violations".
# Try to return the list for the IDs of the rules violated by a given classification made by an AI agent.
# Raise an error if occurs. 
@app.post("/policy-violations")
def policy_violations_endpoint(classification: dict):

    try:
        rule_ids = policy_violation_rule_ids(classification)
        return {
            "policyViolationRuleIds": rule_ids,
        }

    except Exception as error:
        raise HTTPException(
            status_code = 500,
            detail = {
                "status": "policy_violation_parse_error",
                "message": "decision-service failed while extracting policy violation rule ids.",
                "error": str(error),
            },
        )




# POST request: "/payload-instructions".
# Try to return whether a given invoice has instructions which try to influence the decision of the system improperly.
# Raise an error if occurs. 
@app.post("/payload-instructions")
def payload_instructions_endpoint(invoice: dict):

    try:
        signal = detect_payload_instructions(invoice)
        return {
            "payloadInstructionSignal": signal,
        }

    except Exception as error:
        raise HTTPException(
            status_code = 500,
            detail = {
                "status": "payload_instruction_detection_error",
                "message": "decision-service failed while detecting payload instructions.",
                "error": str(error),
            },
        )




# POST request: "/preprocess".
# Try to check global rules, check whether there is a duplication, and return a corresponding result for a given invoice.
# Try to log a corresponding message.
# Raise an error if occurs. 
@app.post("/preprocess")
def preprocess_invoice_endpoint(invoice: dict):

    try:
        
        preprocess = check_global_rules(invoice)
        signature = preprocess.get("signature")

        if signature:
            
            processed_key = f"processed-signature:{signature}"

            try:
                processed_record = dapr_get_state(
                    key = processed_key,
                    default = None,
                )
                
            except DaprClientError:
                processed_record = None

            if processed_record is not None:
                
                preprocess["duplicate"] = True

                hard_stops = preprocess.get("hardStops", [])

                if "GLOBAL-DUP" not in hard_stops:
                    hard_stops.append("GLOBAL-DUP")

                preprocess["hardStops"] = hard_stops

        log_event(
            service = "decision-service",
            event = "invoice_preprocess_completed",
            correlation_id = invoice.get("correlationId", "unknown"),
            tracking_id = invoice.get("trackingId", "unknown"),
            message = "Invoice preprocess/global rules completed.",
            amountUsd = preprocess.get("amountUsd"),
            duplicate = preprocess.get("duplicate"),
            hardStops = preprocess.get("hardStops", []),
            signature = preprocess.get("signature"),
        )

        return {
            "preprocess": preprocess,
        }

    except Exception as error:
        raise HTTPException(
            status_code = 500,
            detail = {
                "status": "decision_preprocess_error",
                "message": "decision-service failed during preprocess/global rules.",
                "error": str(error),
            },
        )




# POST request: "/processed-signatures".
# Try to save the signature of a given invoice as processed for idempotency and duplicate detection.
# Try to persist the signature in a Dapr state so duplicate detection survives service restart.
# Try to log a corresponding message.
# Raise an error if occurs. 
@app.post("/processed-signatures")
def processed_signatures_endpoint(payload: dict):


    signature = payload.get("signature")
    correlation_id = payload.get("correlationId", "unknown")

    if not signature:
        return {
            "status": "skipped",
            "message": "No signature was provided.",
        }

    try:
        
        PROCESSED_INVOICE_SIGNATURES.add(signature)
        
        dapr_save_state(
            key = f"processed-signature:{signature}",
            value = {
                "signature": signature,
                "correlationId": correlation_id,
                "processed": True,
            },
        )

        log_event(
            service = "decision-service",
            event = "processed_signature_saved",
            correlation_id = correlation_id,
            tracking_id = correlation_id,
            message = "Processed invoice signature was saved.",
            signature = signature,
        )

        return {
            "status": "saved",
            "signature": signature,
            "correlationId": correlation_id,
        }

    except Exception as error:
        raise HTTPException(
            status_code = 500,
            detail = {
                "status": "processed_signature_save_error",
                "message": "decision-service failed while saving processed signature.",
                "error": str(error),
            },
        )




# POST request: "/processed-signatures/release".
# Release the signature of a given invoice from the in-memory idempotency collection, if exists there.
# Else do nothing.
# Anyway return a corresponding result.
@app.post("/processed-signatures/release")
def release_processed_signature(payload: dict):
    
    signature = payload.get("signature")
    correlation_id = payload.get("correlationId")

    if not signature:
        return {
            "status": "bad_request",
            "message": "signature is required.",
        }

    if isinstance(PROCESSED_INVOICE_SIGNATURES, set):
        PROCESSED_INVOICE_SIGNATURES.discard(signature)

    elif isinstance(PROCESSED_INVOICE_SIGNATURES, dict):
        
        existing_correlation_id = PROCESSED_INVOICE_SIGNATURES.get(signature)

        if existing_correlation_id is None:
            pass
        
        elif correlation_id is None or existing_correlation_id == correlation_id:
            PROCESSED_INVOICE_SIGNATURES.pop(signature, None)

    return {
        "status": "released",
        "signature": signature,
        "correlationId": correlation_id,
    }