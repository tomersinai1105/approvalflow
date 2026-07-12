# Dapr helper functions for workflow itself. 




import os

from uuid import uuid4
from datetime import datetime, UTC

from app.common.dapr_client import (
    dapr_invoke_service,
    dapr_save_state,
    dapr_get_state,
    dapr_get_secret_or_env,
    DaprClientError,
)

from app.common.logger import log_event




DECISION_SERVICE_APP_ID = os.getenv("DECISION_SERVICE_APP_ID", "decision-service")
PAYMENT_SERVICE_APP_ID = os.getenv("PAYMENT_SERVICE_APP_ID", "payment-service")
AUDIT_SERVICE_APP_ID = os.getenv("AUDIT_SERVICE_APP_ID", "audit-service")

WORKFLOW_STATE_KEY_PREFIX = "workflow"
HUMAN_REVIEW_KEY_PREFIX = "human-review"
WORKFLOW_INDEX_KEY = "workflow:index"
HUMAN_REVIEW_INDEX_KEY = "human-review:index"




# Return OpenAI API key.
def get_openai_api_key() -> str | None:
    return dapr_get_secret_or_env("OPENAI_API_KEY", None)

# Return LLM model.
def get_llm_model() -> str:
    return dapr_get_secret_or_env("LLM_MODEL", "gpt-5.2").strip()

# Return LLM provider.
def get_llm_provider() -> str:
    return dapr_get_secret_or_env("LLM_PROVIDER", "openai").strip().lower()




# Load and return the index list of a given index key from the Dapr state store, if the list exists, else return an empty list.
def load_index(index_key: str) -> list[str]:
    
    try:
        value = dapr_get_state(
            index_key, 
            default = [])
        
    except DaprClientError:
        return []

    if isinstance(value, list):
        return value

    return []




# Try to save a given index list under a given index key in the Dapr state store.
# If error occurs log a corresponding message. 
def save_index(index_key: str, values: list[str]) -> None:
    
    try:
        dapr_save_state(index_key, sorted(set(values)))
        
    except DaprClientError as error:
        log_event(
            service = "workflow-dapr",
            event = "index_save_failed",
            correlation_id = "system",
            tracking_id = "system",
            level = "WARNING",
            message = "Could not save index to Dapr state.",
            indexKey = index_key,
            error = str(error),
        )




# Add a given correlation ID, if not already exists there, to the index list of a given index key. 
def add_to_index(index_key: str, correlation_id: str) -> None:
    values = load_index(index_key)
    if correlation_id not in values:
        values.append(correlation_id)
        save_index(index_key, values)




# Save a given workflow state in a given local cache and try also in the Dapr state store using a given correlation ID.
# If an error occurs log a corresponding message.
def save_workflow_state(
    correlation_id: str,
    state: dict,
    local_cache: dict,
) -> None:

    local_cache[correlation_id] = state
    add_to_index(WORKFLOW_INDEX_KEY, correlation_id)

    try:
        dapr_save_state(
            key = f"{WORKFLOW_STATE_KEY_PREFIX}:{correlation_id}",
            value = state,
        )

    except DaprClientError as error:
        log_event(
            service = "workflow-dapr",
            event = "workflow_state_save_failed",
            correlation_id = correlation_id,
            tracking_id = state.get("submit", {}).get("trackingId", correlation_id),
            level = "WARNING",
            message = "Workflow state could not be saved to Dapr state.",
            error = str(error),
        )




# Return the workflow state of a given correlation ID from the local cache, if exists there.
# Else return the workflow state of the correlation ID from the Dapr state store.
# If an error occurs log a corresponding message.
def get_workflow_state(
    correlation_id: str,
    local_cache: dict,
) -> dict | None:

    if correlation_id in local_cache:
        return local_cache[correlation_id]

    try:
        state = dapr_get_state(
            key = f"{WORKFLOW_STATE_KEY_PREFIX}:{correlation_id}",
            default = None,
        )
        
    except DaprClientError:
        state = None

    if state is not None:
        local_cache[correlation_id] = state

    return state




# Save a given human review task in a given local cache and try also in the Dapr state store using a given correlation ID.
# If an error occurs log a corresponding message.
def save_human_review_task(
    correlation_id: str,
    task: dict,
    local_cache: dict,
) -> None:

    local_cache[correlation_id] = task
    add_to_index(HUMAN_REVIEW_INDEX_KEY, correlation_id)

    try:
        dapr_save_state(
            key = f"{HUMAN_REVIEW_KEY_PREFIX}:{correlation_id}",
            value = task,
        )

    except DaprClientError as error:
        log_event(
            service = "workflow-dapr",
            event = "human_review_state_save_failed",
            correlation_id = correlation_id,
            tracking_id = task.get("trackingId", correlation_id),
            level = "WARNING",
            message = "Human review task could not be saved to Dapr state.",
            error = str(error),
        )




# Return the human review task of a given correlation ID from the local cache, if exists there.
# Else return the human review task of the correlation ID from the Dapr state store.
# If an error occurs log a corresponding message.
def get_human_review_task(
    correlation_id: str,
    local_cache: dict,
) -> dict | None:

    if correlation_id in local_cache:
        return local_cache[correlation_id]

    try:
        task = dapr_get_state(
            key=f"{HUMAN_REVIEW_KEY_PREFIX}:{correlation_id}",
            default=None,
        )
        
    except DaprClientError:
        task = None

    if task is not None:
        local_cache[correlation_id] = task

    return task




# Load all the workflow states in a given local cache.
def load_all_workflow_states(local_cache: dict) -> dict:
    """
    Load all known workflow states for dashboard metrics.
    """

    states = dict(local_cache)

    for correlation_id in load_index(WORKFLOW_INDEX_KEY):
        
        if correlation_id not in states:
            
            state = get_workflow_state(correlation_id, local_cache)
            
            if state is not None:
                states[correlation_id] = state

    return states




# Load all the human review tasks in a given local cache.
def load_all_human_review_tasks(local_cache: dict) -> dict:

    tasks = dict(local_cache)

    for correlation_id in load_index(HUMAN_REVIEW_INDEX_KEY):
        
        if correlation_id not in tasks:
            
            task = get_human_review_task(correlation_id, local_cache)

            if task is not None:
                tasks[correlation_id] = task

    return tasks




# Load the runtime policy config from decision-service through Dapr.
# If an error occurs log a corresponding message.
def dapr_get_runtime_policy_config(default_autonomy_ceiling: float, default_autonomy_confidence: float,) -> dict:

    try:
        
        response = dapr_invoke_service(
            app_id = DECISION_SERVICE_APP_ID,
            method_name = "config",
            payload = None,
            http_method = "GET",
        )

        config = response.get("config", response)

        return {
            "autonomyCeiling": float(
                config.get("autonomyCeiling", default_autonomy_ceiling)
            ),
            "autonomyConfidence": float(
                config.get("autonomyConfidence", default_autonomy_confidence)
            ),
        }

    except Exception as error:
        log_event(
            service = "workflow-dapr",
            event = "runtime_policy_config_load_failed",
            correlation_id = "system",
            tracking_id = "system",
            level = "WARNING",
            message = "Could not load runtime policy config from decision-service. Falling back to defaults.",
            error = str(error),
        )

        return {
            "autonomyCeiling": default_autonomy_ceiling,
            "autonomyConfidence": default_autonomy_confidence,
        }




# Validate a given invoice. 
def dapr_validate_invoice_input(invoice: dict) -> dict:
    
    response = dapr_invoke_service(
        app_id = DECISION_SERVICE_APP_ID,
        method_name = "validate",
        payload = invoice,
        http_method = "POST",
    )

    return response["validation"]




# Check global rules and return a corresponding result for a given invoice.  
def dapr_check_global_rules(invoice: dict) -> dict:
    
    response = dapr_invoke_service(
        app_id = DECISION_SERVICE_APP_ID,
        method_name="preprocess",
        payload = invoice,
        http_method = "POST",
    )

    return response["preprocess"]




# Return the list for the IDs of the rules violated by a given classification made by an AI agent.
def dapr_policy_violation_rule_ids(classification: dict) -> list[str]:
    
    response = dapr_invoke_service(
        app_id = DECISION_SERVICE_APP_ID,
        method_name = "policy-violations",
        payload = classification,
        http_method = "POST",
    )

    return response["policyViolationRuleIds"]




# Return whether a given invoice has instructions which try to influence the decision of the system improperly.
def dapr_detect_payload_instructions(invoice: dict) -> bool:
    
    response = dapr_invoke_service(
        app_id = DECISION_SERVICE_APP_ID,
        method_name = "payload-instructions",
        payload = invoice,
        http_method = "POST",
    )

    return bool(response["payloadInstructionSignal"])




# Mark a given invoice signature as processed, using a given correlation ID.
def dapr_mark_signature_processed(signature: str, correlation_id: str) -> None:

    dapr_invoke_service(
        app_id = DECISION_SERVICE_APP_ID,
        method_name = "processed-signatures",
        payload = {
            "signature": signature,
            "correlationId": correlation_id,
        },
        http_method = "POST",
    )




# Release a given processed invoice signature, using a given correlation ID.
def dapr_release_signature_processed(signature: str, correlation_id: str) -> None:

    dapr_invoke_service(
        app_id = DECISION_SERVICE_APP_ID,
        method_name = "processed-signatures/release",
        payload = {
            "signature": signature,
            "correlationId": correlation_id,
        },
        http_method = "POST",
    )




# Return a final result for audit trail based on given details.
def dapr_build_final_result(
    router_response: dict,
    invoice: dict,
    status: str,
    message: str,
    final_decision_made_by: str,
    payment_saga: dict | None = None,
    human_decision: dict | None = None,
    human_review_task: dict | None = None,
) -> dict:
    
    response = dapr_invoke_service(
        app_id = AUDIT_SERVICE_APP_ID,
        method_name = "final-result",
        payload = {
            "routerResponse": router_response,
            "invoice": invoice,
            "status": status,
            "message": message,
            "finalDecisionMadeBy": final_decision_made_by,
            "paymentSaga": payment_saga,
            "humanDecision": human_decision,
            "humanReviewTask": human_review_task,
        },
        http_method="POST",
    )

    return response["result"]




# Run and return a payment saga based on given details. 
# Log a corresponding message.  
def run_payment_saga_via_dapr(
    correlation_id: str,
    tracking_id: str,
    invoice: dict,
    amount_usd: float,
) -> dict:
    
    log_event(
        service = "workflow-dapr",
        event = "payment_service_invocation_started",
        correlation_id = correlation_id,
        tracking_id = tracking_id,
        message = "Workflow is invoking payment-service through Dapr.",
        paymentServiceAppId = PAYMENT_SERVICE_APP_ID,
        amountUsd = amount_usd,
        department = invoice.get("department"),
    )

    try:
        payment_saga = dapr_invoke_service(
            app_id = PAYMENT_SERVICE_APP_ID,
            method_name = "payment/saga",
            payload = {
                "correlationId": correlation_id,
                "trackingId": tracking_id,
                "invoice": invoice,
                "amountUsd": amount_usd,
            },
            http_method = "POST",
        )

        log_event(
            service = "workflow-dapr",
            event = "payment_service_invocation_completed",
            correlation_id = correlation_id,
            tracking_id = tracking_id,
            message = "Payment-service returned a response through Dapr.",
            paymentServiceAppId = PAYMENT_SERVICE_APP_ID,
            sagaStatus = payment_saga.get("status"),
        )

        return payment_saga

    except Exception as error:
        log_event(
            service = "workflow-dapr",
            event = "payment_service_invocation_failed",
            correlation_id = correlation_id,
            tracking_id = tracking_id,
            level = "ERROR",
            message = "Workflow failed to invoke payment-service through Dapr.",
            paymentServiceAppId = PAYMENT_SERVICE_APP_ID,
            error = str(error),
        )

        return {
            "status": "payment_service_error",
            "message": "Payment service invocation failed through Dapr.",
            "payment": {
                "status": "payment_service_error",
                "reason": str(error),
            },
            "reservation": None,
            "budgetCommitment": None,
            "compensation": None,
        }




# Submit an initial submission and return a local immediate submission acknowledgement.

def create_local_submit_response() -> dict:

    tracking_id = f"INV-{uuid4().hex[:8].upper()}"
    correlation_id = tracking_id

    return {
        "trackingId": tracking_id,
        "correlationId": correlation_id,
        "status": "accepted",
        "receivedAt": datetime.now(UTC).isoformat(),
        "message": "Invoice accepted for local workflow processing.",
    }