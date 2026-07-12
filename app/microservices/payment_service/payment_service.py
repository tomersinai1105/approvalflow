# Payments service for payment saga.



import os
import json
from datetime import datetime, UTC
from uuid import uuid4

from app.common.dapr_client import (
    dapr_save_state,
    dapr_get_state,
    DaprClientError,
)

from app.common.logger import log_event




SAMPLE_INVOICES_PATH = os.getenv(
    "SAMPLE_INVOICES_PATH",
    "config/sample-invoices.json",
)

DEFAULT_BUDGETS = {
    "sales": 10000.0,
    "engineering": 15000.0,
    "finance": 8000.0,
    "marketing": 12000.0,
    "hr": 6000.0,
    "operations": 10000.0,
}

BUDGETS = {}

BUDGET_RESERVATIONS = {}

PAYMENTS = {}

PAYMENT_SAGAS = {}




# Return a normalized version of a given department. 
def normalize_department(department: str | None) -> str:
    
    if not department:
        return "unknown"

    return str(department).strip().lower()




# Return a given value converted to a float number. 
def to_float(value, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default




# Try to return a list of the sample invoices budges.
# In a failure scenario, return a list of the default budges. 
def load_budget_seed() -> dict:

    try:
        
        with open(SAMPLE_INVOICES_PATH, "r", encoding = "utf-8") as file:
            data = json.load(file)

        budgets = data.get("budgets", {})

        if isinstance(budgets, dict) and budgets:
            
            normalized = {}

            for department, amount in budgets.items():
                normalized[normalize_department(department)] = to_float(amount)

            return normalized

    except FileNotFoundError:
        pass

    except Exception:
        pass

    return dict(DEFAULT_BUDGETS)




# Returns whether a payment failure for a given invoice should be simulated.
def should_simulate_payment_failure(invoice: dict) -> bool:

    explicit_flags = [
        "simulatePaymentFailure",
        "forcePaymentFailure",
        "paymentShouldFail",
        "failPayment",
    ]

    for flag in explicit_flags:
        if invoice.get(flag) is True:
            return True

    notes = str(invoice.get("notes", "")).lower()
    scenario = str(invoice.get("scenario", "")).lower()
    invoice_id = str(invoice.get("id", "")).upper()

    return (
        "payment failure" in notes
        or "force payment failure" in notes
        or "simulate payment failure" in notes
        or "payment_failure" in scenario
        or invoice_id == "INV-1012"
    )




# If a given cache key is in a given cache with a value, return the value.
# Else if a given Dapr key is in the Dapr state store with a value, save the cache key with the value in the cache and return the value.
# Else return a given default. 
def get_state_or_cache(key: str, 
    cache: dict, 
    cache_key: str, 
    default = None,
):

    if cache_key in cache:
        return cache[cache_key]

    try:
        value = dapr_get_state(
            key = key,
            default = None,
        )
        
    except DaprClientError:
        value = None

    if value is None:
        return default

    cache[cache_key] = value
    return value




# Save a given value under a given cache key in a given cache.
# Try to save a JSON based on a given key and the value in the Dapr state store.
# If an error occurs, log a corresponding message. 
def save_state_and_cache(key: str, 
    value: dict, 
    cache: dict, 
    cache_key: str,
) -> None:

    cache[cache_key] = value

    try:
        dapr_save_state(
            key = key,
            value = value,
        )
        
    except DaprClientError as error:
        log_event(
            service = "payment-service",
            event = "dapr_state_save_failed",
            correlation_id = value.get("correlationId", "system"),
            tracking_id = value.get("trackingId", "system"),
            level = "WARNING",
            message = "Payment service could not save state to Dapr.",
            stateKey = key,
            error = str(error),
        )




# If a given department has a budget in the Dapr state store, return the budget.
# Else initialize and return a budget from the sample invoices list, if exists.
# Else initialize and return a budget from the default budgets list. 
def get_budget(department: str) -> dict:

    department = normalize_department(department)
    key = f"budget:{department}"

    budget = get_state_or_cache(
        key = key,
        cache = BUDGETS,
        cache_key = department,
        default = None,
    )

    if budget is not None:
        return budget

    seed = load_budget_seed()
    initial_amount = to_float(seed.get(department, 0.0))

    budget = {
        "department": department,
        "initialUsd": initial_amount,
        "availableUsd": initial_amount,
        "reservedUsd": 0.0,
        "spentUsd": 0.0,
        "createdAt": datetime.now(UTC).isoformat(),
        "updatedAt": datetime.now(UTC).isoformat(),
    }

    save_state_and_cache(
        key = key,
        value = budget,
        cache = BUDGETS,
        cache_key = department,
    )

    return budget




# Save a given budget in the budgets cache and in the Dapr state store.
def save_budget(budget: dict) -> None:
    
    department = normalize_department(budget.get("department"))
    budget["department"] = department
    budget["updatedAt"] = datetime.now(UTC).isoformat()

    save_state_and_cache(
        key = f"budget:{department}",
        value = budget,
        cache = BUDGETS,
        cache_key = department,
    )




# Reserve and return a budget based on given details.
# Log a corresponding message.  
def reserve_budget(
    correlation_id: str,
    tracking_id: str,
    invoice: dict,
    amount_usd: float,
) -> dict:
    
    existing = get_state_or_cache(
        key = f"reservation:{correlation_id}",
        cache = BUDGET_RESERVATIONS,
        cache_key = correlation_id,
        default = None,
    )

    if existing is not None:
        return existing

    department = normalize_department(invoice.get("department"))
    budget = get_budget(department)

    available = to_float(budget.get("availableUsd"))

    if amount_usd <= 0:
        
        reservation = {
            "reservationId": f"RES-{uuid4().hex[:8].upper()}",
            "correlationId": correlation_id,
            "trackingId": tracking_id,
            "department": department,
            "amountUsd": amount_usd,
            "status": "rejected",
            "reason": "Reservation amount must be greater than zero.",
            "createdAt": datetime.now(UTC).isoformat(),
            "updatedAt": datetime.now(UTC).isoformat(),
        }

        save_state_and_cache(
            key = f"reservation:{correlation_id}",
            value = reservation,
            cache = BUDGET_RESERVATIONS,
            cache_key = correlation_id,
        )

        return reservation

    if amount_usd > available:
        
        reservation = {
            "reservationId": f"RES-{uuid4().hex[:8].upper()}",
            "correlationId": correlation_id,
            "trackingId": tracking_id,
            "department": department,
            "amountUsd": amount_usd,
            "status": "rejected",
            "reason": (
                f"Insufficient budget. Requested ${amount_usd:.2f}, "
                f"available ${available:.2f}."
            ),
            "createdAt": datetime.now(UTC).isoformat(),
            "updatedAt": datetime.now(UTC).isoformat(),
        }

        save_state_and_cache(
            key = f"reservation:{correlation_id}",
            value = reservation,
            cache = BUDGET_RESERVATIONS,
            cache_key = correlation_id,
        )

        log_event(
            service = "payment-service",
            event = "budget_reservation_rejected",
            correlation_id = correlation_id,
            tracking_id = tracking_id,
            message = "Budget reservation was rejected.",
            department = department,
            requestedAmountUsd = amount_usd,
            availableBudgetUsd = available,
        )

        return reservation

    budget["availableUsd"] = round(available - amount_usd, 2)
    budget["reservedUsd"] = round(to_float(budget.get("reservedUsd")) + amount_usd, 2)

    save_budget(budget)

    reservation = {
        "reservationId": f"RES-{uuid4().hex[:8].upper()}",
        "correlationId": correlation_id,
        "trackingId": tracking_id,
        "department": department,
        "amountUsd": amount_usd,
        "status": "reserved",
        "reason": "Budget was reserved successfully.",
        "createdAt": datetime.now(UTC).isoformat(),
        "updatedAt": datetime.now(UTC).isoformat(),
    }

    save_state_and_cache(
        key = f"reservation:{correlation_id}",
        value = reservation,
        cache = BUDGET_RESERVATIONS,
        cache_key = correlation_id,
    )

    log_event(
        service = "payment-service",
        event = "budget_reserved",
        correlation_id = correlation_id,
        tracking_id = tracking_id,
        message = "Budget was reserved.",
        department = department,
        amountUsd = amount_usd,
        availableAfterReservation = budget.get("availableUsd"),
    )

    return reservation




# Release and return a reserved budget based on given details if payment failed.
# Log a corresponding message.  
def release_budget_reservation(
    correlation_id: str,
    tracking_id: str,
    reason: str,
) -> dict | None:

    reservation = get_state_or_cache(
        key = f"reservation:{correlation_id}",
        cache = BUDGET_RESERVATIONS,
        cache_key = correlation_id,
        default = None,
    )

    if reservation is None:
        return None

    if reservation.get("status") != "reserved":
        return reservation

    department = normalize_department(reservation.get("department"))
    amount_usd = to_float(reservation.get("amountUsd"))

    budget = get_budget(department)

    budget["availableUsd"] = round(to_float(budget.get("availableUsd")) + amount_usd, 2)
    budget["reservedUsd"] = round(max(0.0, to_float(budget.get("reservedUsd")) - amount_usd), 2)

    save_budget(budget)

    reservation["status"] = "released"
    reservation["releaseReason"] = reason
    reservation["releasedAt"] = datetime.now(UTC).isoformat()
    reservation["updatedAt"] = datetime.now(UTC).isoformat()

    save_state_and_cache(
        key = f"reservation:{correlation_id}",
        value = reservation,
        cache = BUDGET_RESERVATIONS,
        cache_key = correlation_id,
    )

    log_event(
        service = "payment-service",
        event = "budget_reservation_released",
        correlation_id = correlation_id,
        tracking_id = tracking_id,
        message = "Budget reservation was released as compensation.",
        department = department,
        amountUsd = amount_usd,
        reason = reason,
    )

    return reservation




# Commit and return a reserved budget based on given details after payment succeeded.   
# Log a corresponding message.  
def commit_budget_reservation(
    correlation_id: str,
    tracking_id: str,
) -> dict | None:

    reservation = get_state_or_cache(
        key = f"reservation:{correlation_id}",
        cache = BUDGET_RESERVATIONS,
        cache_key = correlation_id,
        default = None,
    )

    if reservation is None:
        return None

    if reservation.get("status") == "committed":
        return reservation

    if reservation.get("status") != "reserved":
        return reservation

    department = normalize_department(reservation.get("department"))
    amount_usd = to_float(reservation.get("amountUsd"))

    budget = get_budget(department)

    budget["reservedUsd"] = round(max(0.0, to_float(budget.get("reservedUsd")) - amount_usd), 2)
    budget["spentUsd"] = round(to_float(budget.get("spentUsd")) + amount_usd, 2)

    save_budget(budget)

    reservation["status"] = "committed"
    reservation["committedAt"] = datetime.now(UTC).isoformat()
    reservation["updatedAt"] = datetime.now(UTC).isoformat()

    save_state_and_cache(
        key = f"reservation:{correlation_id}",
        value = reservation,
        cache = BUDGET_RESERVATIONS,
        cache_key = correlation_id,
    )

    log_event(
        service = "payment-service",
        event = "budget_reservation_committed",
        correlation_id = correlation_id,
        tracking_id = tracking_id,
        message = "Budget reservation was committed after successful payment.",
        department = department,
        amountUsd = amount_usd,
    )

    return reservation




# Execute and return a payment based on given details. 
# Log a corresponding message.  
def execute_payment(
    correlation_id: str,
    tracking_id: str,
    invoice: dict,
    amount_usd: float,
) -> dict:

    existing = get_state_or_cache(
        key = f"payment:{correlation_id}",
        cache = PAYMENTS,
        cache_key = correlation_id,
        default = None,
    )

    if existing is not None:
        return existing

    if should_simulate_payment_failure(invoice):
        
        payment = {
            "paymentId": f"PAY-{uuid4().hex[:8].upper()}",
            "correlationId": correlation_id,
            "trackingId": tracking_id,
            "status": "failed",
            "amountUsd": amount_usd,
            "reason": "Simulated payment failure for verification scenario.",
            "createdAt": datetime.now(UTC).isoformat(),
            "updatedAt": datetime.now(UTC).isoformat(),
        }

        save_state_and_cache(
            key = f"payment:{correlation_id}",
            value = payment,
            cache = PAYMENTS,
            cache_key = correlation_id,
        )

        log_event(
            service = "payment-service",
            event = "payment_failed",
            correlation_id = correlation_id,
            tracking_id = tracking_id,
            level = "ERROR",
            message = "Payment failed.",
            amountUsd = amount_usd,
            reason = payment.get("reason"),
        )

        return payment

    payment = {
        "paymentId": f"PAY-{uuid4().hex[:8].upper()}",
        "correlationId": correlation_id,
        "trackingId": tracking_id,
        "status": "paid",
        "amountUsd": amount_usd,
        "reason": "Payment completed successfully.",
        "createdAt": datetime.now(UTC).isoformat(),
        "updatedAt": datetime.now(UTC).isoformat(),
    }

    save_state_and_cache(
        key = f"payment:{correlation_id}",
        value = payment,
        cache = PAYMENTS,
        cache_key = correlation_id,
    )

    log_event(
        service = "payment-service",
        event = "payment_paid",
        correlation_id = correlation_id,
        tracking_id = tracking_id,
        message = "Payment completed successfully.",
        amountUsd = amount_usd,
    )

    return payment




# Run and return a payment saga based on given details. 
# Log a corresponding message.  
def run_payment_saga(
    correlation_id: str,
    tracking_id: str,
    invoice: dict,
    amount_usd: float,
) -> dict:

    existing_saga = get_state_or_cache(
        key = f"payment-saga:{correlation_id}",
        cache = PAYMENT_SAGAS,
        cache_key = correlation_id,
        default = None,
    )

    if existing_saga is not None and existing_saga.get("status") in [
        "paid",
        "payment_failed",
        "budget_rejected",
        "payment_service_error",
    ]:
        log_event(
            service = "payment-service",
            event = "payment_saga_idempotent_return",
            correlation_id = correlation_id,
            tracking_id = tracking_id,
            message = "Existing payment saga was returned idempotently.",
            sagaStatus = existing_saga.get("status"),
        )

        return existing_saga

    amount_usd = round(to_float(amount_usd), 2)

    saga = {
        "correlationId": correlation_id,
        "trackingId": tracking_id,
        "status": "started",
        "message": "Payment saga started.",
        "amountUsd": amount_usd,
        "invoiceNumber": invoice.get("invoiceNumber"),
        "vendor": invoice.get("vendor"),
        "department": normalize_department(invoice.get("department")),
        "reservation": None,
        "payment": None,
        "budgetCommitment": None,
        "compensation": None,
        "createdAt": datetime.now(UTC).isoformat(),
        "updatedAt": datetime.now(UTC).isoformat(),
    }

    save_state_and_cache(
        key = f"payment-saga:{correlation_id}",
        value = saga,
        cache = PAYMENT_SAGAS,
        cache_key = correlation_id,
    )

    reservation = reserve_budget(
        correlation_id = correlation_id,
        tracking_id = tracking_id,
        invoice = invoice,
        amount_usd = amount_usd,
    )

    saga["reservation"] = reservation
    saga["updatedAt"] = datetime.now(UTC).isoformat()

    if reservation.get("status") != "reserved":
        saga["status"] = "budget_rejected"
        saga["message"] = reservation.get("reason", "Budget reservation failed.")

        save_state_and_cache(
            key = f"payment-saga:{correlation_id}",
            value = saga,
            cache = PAYMENT_SAGAS,
            cache_key = correlation_id,
        )

        return saga

    payment = execute_payment(
        correlation_id = correlation_id,
        tracking_id = tracking_id,
        invoice = invoice,
        amount_usd = amount_usd,
    )

    saga["payment"] = payment
    saga["updatedAt"] = datetime.now(UTC).isoformat()

    if payment.get("status") != "paid":
        compensation = release_budget_reservation(
            correlation_id = correlation_id,
            tracking_id = tracking_id,
            reason = payment.get("reason", "Payment failed."),
        )

        saga["status"] = "payment_failed"
        saga["message"] = "Payment failed. Budget reservation was released."
        saga["compensation"] = compensation
        saga["updatedAt"] = datetime.now(UTC).isoformat()

        save_state_and_cache(
            key = f"payment-saga:{correlation_id}",
            value = saga,
            cache = PAYMENT_SAGAS,
            cache_key = correlation_id,
        )

        return saga

    budget_commitment = commit_budget_reservation(
        correlation_id = correlation_id,
        tracking_id = tracking_id,
    )

    saga["status"] = "paid"
    saga["message"] = "Payment completed and budget reservation was committed."
    saga["budgetCommitment"] = budget_commitment
    saga["updatedAt"] = datetime.now(UTC).isoformat()

    save_state_and_cache(
        key = f"payment-saga:{correlation_id}",
        value = saga,
        cache = PAYMENT_SAGAS,
        cache_key = correlation_id,
    )

    return saga