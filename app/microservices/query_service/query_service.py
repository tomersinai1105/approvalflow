# Query service for a human-escalated items queue and a dashboard. 




from app.common.dapr_client import (
    dapr_get_state,
    DaprClientError,
)




WORKFLOW_INDEX_KEY = "workflow:index"
HUMAN_REVIEW_INDEX_KEY = "human-review:index"




# Load and return an index list under a given index key from the Dapr state store, if the list exists, else return an empty list.
def load_index(index_key: str) -> list[str]:

    try:
        value = dapr_get_state(
            key = index_key,
            default = [],
        )

    except DaprClientError:
        return []

    if isinstance(value, list):
        return value

    return []




# Try to and returns the workflow state of a given correlation ID from the Dapr state store.
# Raise an error if occurs.
def load_workflow_state(correlation_id: str) -> dict | None:
    try:
        return dapr_get_state(
            key = f"workflow:{correlation_id}",
            default = None,
        )

    except DaprClientError:
        return None




# Try to load a human review task of a given correlation ID from the Dapr state store.
# Raise an error if occurs. 
def load_human_review_task(correlation_id: str) -> dict | None:
    
    try:
        return dapr_get_state(
            key = f"human-review:{correlation_id}",
            default = None,
        )

    except DaprClientError:
        return None




# Load all the workflow states in the Dapr state store. 
def load_all_workflow_states() -> dict:
    
    states = {}

    for correlation_id in load_index(WORKFLOW_INDEX_KEY):
        state = load_workflow_state(correlation_id)

        if state is not None:
            states[correlation_id] = state

    return states




# Load all the human review tasks in the Dapr state store. 
def load_all_human_review_tasks() -> dict:
    
    tasks = {}

    for correlation_id in load_index(HUMAN_REVIEW_INDEX_KEY):
        task = load_human_review_task(correlation_id)

        if task is not None:
            tasks[correlation_id] = task

    return tasks




# Return a queue of all human-escalated items. 
def get_human_review_queue() -> list[dict]:


    tasks = load_all_human_review_tasks()
    
    queue = []

    for task in tasks.values():
        
        status = task.get("status")

        if status not in ["waiting_for_human_review", "waiting_for_info"]:
            continue

        queue.append(
            {
                "trackingId": task.get("trackingId"),
                "correlationId": task.get("correlationId"),
                "status": status,
                "vendor": task.get("vendor"),
                "invoiceNumber": task.get("invoiceNumber"),
                "department": task.get("department"),
                "category": task.get("category"),
                "amountUsd": task.get("amountUsd"),
                "agentRecommendation": task.get("agentRecommendation"),
                "agentConfidence": task.get("agentConfidence"),
                "citedRules": task.get("citedRules", []),
                "policyViolations": task.get("policyViolations", []),
                "appliedRules": task.get("appliedRules", []),
                "reason": task.get("reason"),
                "availableActions": task.get(
                    "availableActions",
                    ["approve", "reject", "request_info"],
                ),
                "createdAt": task.get("createdAt"),
                "humanDecision": task.get("humanDecision"),
            }
        )

    return queue




# Return a dashboard. 
def get_dashboard_metrics() -> dict:

    states = load_all_workflow_states()
    total_items = len(states)
    auto_approved_count = 0
    human_review_count = 0
    human_approved_count = 0
    rejected_count = 0
    duplicate_count = 0
    payment_failed_count = 0
    auto_approved_money = 0.0
    human_approved_money = 0.0

    for state in states.values():
        
        final = state.get("final") or {}
        router_response = state.get("routerResponse") or {}
        preprocess = state.get("preprocess") or {}
        payment_saga = state.get("paymentSaga") or {}
        final_route = final.get("finalRoute") or router_response.get("finalRoute")
        status = final.get("status") or state.get("status")
        final_decision_made_by = final.get("finalDecisionMadeBy")

        try:
            amount_usd = float(preprocess.get("amountUsd", 0.0))
        except (TypeError, ValueError):
            amount_usd = 0.0

        if final_route == "auto_approve":
            auto_approved_count += 1

            if payment_saga.get("status") == "paid" or status == "paid":
                auto_approved_money += amount_usd

        elif final_route == "human_review":
            
            human_review_count += 1

            if (
                payment_saga.get("status") == "paid"
                and final_decision_made_by
                and final_decision_made_by != "deterministic_router"
            ):
                human_approved_count += 1
                human_approved_money += amount_usd

        elif final_route == "reject":
            rejected_count += 1

        elif final_route == "duplicate":
            duplicate_count += 1

        if status in ["payment_failed", "payment_service_error"]:
            payment_failed_count += 1

    return {
        "throughput": total_items,
        "totalItems": total_items,
        "autoApprovedCount": auto_approved_count,
        "humanReviewCount": human_review_count,
        "humanApprovedCount": human_approved_count,
        "rejectedCount": rejected_count,
        "duplicateCount": duplicate_count,
        "paymentFailedCount": payment_failed_count,
        "autoApprovedMoneyUsd": round(auto_approved_money, 2),
        "humanApprovedMoneyUsd": round(human_approved_money, 2),
        "autoApprovalRate": (
            round(auto_approved_count / total_items, 4)
            if total_items > 0
            else 0
        ),
        "humanReviewRate": (
            round(human_review_count / total_items, 4)
            if total_items > 0
            else 0
        ),
        "rejectionRate": (
            round(rejected_count / total_items, 4)
            if total_items > 0
            else 0
        ),
    }