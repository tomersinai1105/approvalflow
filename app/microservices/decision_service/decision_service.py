# Decision service for the processor to take an initial decision in the workflow.




import os

import re

from datetime import datetime

from app.common.dapr_client import (
    dapr_get_state,
    DaprClientError,
)




REQUIRED_INVOICE_FIELDS = [
    "submitter",
    "department",
    "vendor",
    "vendorKnown",
    "invoiceNumber",
    "currency",
    "category",
    "lineItems",
    "taxAmount",
    "total",
    "receiptPresent",
    "date",
]

REQUIRED_LINE_ITEM_FIELDS = [
    "description",
    "quantity",
    "unitPrice",
]

ALLOWED_DEPARTMENTS = [
    "engineering-2026Q2",
    "sales-2026Q2",
    "marketing-2026Q2",
]

ALLOWED_CURRENCIES = ["USD", "EUR", "GBP"]

FX_RATES = {
    "USD": 1.0,
    "EUR": 1.08,
    "GBP": 1.27,
}

ALLOWED_CATEGORIES = ["meals", "travel", "saas", "hardware", "other"]

CATEGORY_CITED_RULE_IDS = [
    "MEAL-01",
    "MEAL-02",
    "MEAL-03",
    "TRAVEL-01",
    "TRAVEL-02",
    "TRAVEL-03",
    "SAAS-01",
    "HW-01",
    "HW-02",
]

CATEGORY_VIOLATION_RULE_IDS = [
    "MEAL-01",
    "MEAL-02",
    "MEAL-03",
    "TRAVEL-02",
    "TRAVEL-03",
    "SAAS-01",
    "HW-02",
]

AUTONOMY_CEILING = float(os.getenv("AUTONOMY_CEILING", "250.0"))

AUTONOMY_CONFIDENCE = float(os.getenv("AUTONOMY_CONFIDENCE", "0.80"))

RUNTIME_CONFIG_STATE_KEY = "runtime-policy-config"

DEFAULT_RUNTIME_CONFIG = {
    "autonomyCeiling": AUTONOMY_CEILING,
    "autonomyConfidence": AUTONOMY_CONFIDENCE,
}

PROCESSED_INVOICE_SIGNATURES = set()




# Validate a given email. 
def is_valid_email(email: str) -> bool:
    pattern = r"^[^@\s]+@[^@\s]+\.[^@\s]+$"
    return re.match(pattern, email) is not None




# Validate a given invoice. 
def validate_invoice_input(invoice: dict) -> dict:

    errors = []

    if not isinstance(invoice, dict):
        return {
            "valid": False,
            "errors": ["invoice must be a JSON object"],
        }

    for field in REQUIRED_INVOICE_FIELDS:
        if field not in invoice:
            errors.append(f"Missing required field: {field}")

    if errors:
        return {
            "valid": False,
            "errors": errors,
        }

    if not isinstance(invoice["submitter"], str) or invoice["submitter"].strip() == "":
        errors.append("submitter must be a non-empty text")
    elif not is_valid_email(invoice["submitter"]):
        errors.append("submitter must be a valid email address")

    if invoice["department"] not in ALLOWED_DEPARTMENTS:
        errors.append(
            f"Invalid department: {invoice['department']}. "
            f"Allowed values: {ALLOWED_DEPARTMENTS}"
        )

    if not isinstance(invoice["vendor"], str) or invoice["vendor"].strip() == "":
        errors.append("vendor must be a non-empty text")

    if not isinstance(invoice["vendorKnown"], bool):
        errors.append("vendorKnown must be true or false")

    if not isinstance(invoice["invoiceNumber"], str) or invoice["invoiceNumber"].strip() == "":
        errors.append("invoiceNumber must be a non-empty text")

    if invoice["currency"] not in ALLOWED_CURRENCIES:
        errors.append(
            f"Invalid currency: {invoice['currency']}. "
            f"Allowed values: {ALLOWED_CURRENCIES}"
        )

    if invoice["category"] not in ALLOWED_CATEGORIES:
        errors.append(
            f"Invalid category: {invoice['category']}. "
            f"Allowed values: {ALLOWED_CATEGORIES}"
        )

    if invoice["category"] == "meals":
        if "attendees" not in invoice:
            errors.append("attendees is required for meals")
        elif not isinstance(invoice["attendees"], int) or invoice["attendees"] <= 0:
            errors.append("attendees must be a positive whole number")

    if not isinstance(invoice["lineItems"], list) or len(invoice["lineItems"]) == 0:
        errors.append("lineItems must be a non-empty list")
    else:
        for index, item in enumerate(invoice["lineItems"]):
            if not isinstance(item, dict):
                errors.append(f"lineItems[{index}] must be an object")
                continue

            for field in REQUIRED_LINE_ITEM_FIELDS:
                if field not in item:
                    errors.append(f"Missing field in lineItems[{index}]: {field}")

            if "description" in item:
                if not isinstance(item["description"], str) or item["description"].strip() == "":
                    errors.append(f"lineItems[{index}].description must be a non-empty text")

            if "quantity" in item:
                if not isinstance(item["quantity"], (int, float)) or item["quantity"] <= 0:
                    errors.append(f"lineItems[{index}].quantity must be greater than 0")

            if "unitPrice" in item:
                if not isinstance(item["unitPrice"], (int, float)) or item["unitPrice"] < 0:
                    errors.append(f"lineItems[{index}].unitPrice must be zero or positive")

    if not isinstance(invoice["taxAmount"], (int, float)) or invoice["taxAmount"] < 0:
        errors.append("taxAmount must be a zero or positive number")

    if not isinstance(invoice["total"], (int, float)) or invoice["total"] < 0:
        errors.append("total must be a zero or positive number")

    if not isinstance(invoice["receiptPresent"], bool):
        errors.append("receiptPresent must be true or false")

    try:
        invoice_date = datetime.strptime(invoice["date"], "%Y-%m-%d").date()
    except (ValueError, TypeError):
        errors.append("date must be in YYYY-MM-DD format")
    else:
        today = datetime.now().date()
        if invoice_date > today:
            errors.append("date cannot be later than today")

    return {
        "valid": len(errors) == 0,
        "errors": errors,
    }




# Return the signature for a given invoice to be used later to prevent duplicity.  
def build_duplicate_signature(invoice: dict) -> str:
    vendor = invoice["vendor"].strip().lower()
    invoice_number = invoice["invoiceNumber"].strip().lower()
    total = round(invoice["total"], 2)
    return f"{vendor}|{invoice_number}|{total}"




# Return whether a given date is a weekend day, a fraud-pattern signal.
def is_weekend(date: str) -> bool:
    invoice_date = datetime.strptime(date, "%Y-%m-%d")
    return invoice_date.weekday() >= 5




# Return whether a given invoice has a weak line-item description, a fraud-pattern signal.
def has_vague_line_item_detail(invoice: dict) -> bool:

    vague_words = [
        "services",
        "consulting",
        "misc",
        "other",
        "general",
    ]

    for item in invoice["lineItems"]:
        
        description = item["description"].strip().lower()

        if description == "":
            return True

        if any(word in description for word in vague_words):
            return True

    return False




# Return whether a given invoice has padded quantities, a fraud-pattern signal.
def has_padded_quantities(invoice: dict) -> bool:

    for item in invoice["lineItems"]:
        
        quantity = item["quantity"]
        unit_price = item["unitPrice"]

        if quantity >= 10 and unit_price >= 100:
            return True

    return False




# Return whether given invoice and amount USD have fraud signals. 
def detect_fraud_signals(invoice: dict, amount_usd: float) -> dict:

    signals = []

    vendor_is_new = not invoice["vendorKnown"]
    large_round_number = amount_usd >= 1000 and amount_usd % 100 == 0
    vague_detail = has_vague_line_item_detail(invoice)
    submitted_on_weekend = is_weekend(invoice["date"])
    padded_quantities = has_padded_quantities(invoice)

    if vendor_is_new:
        signals.append("new_vendor")

    if large_round_number:
        signals.append("large_round_number")

    if vendor_is_new and large_round_number:
        signals.append("round_number_to_brand_new_vendor")

    if vague_detail:
        signals.append("no_line_item_detail")

    if submitted_on_weekend:
        signals.append("off_hours")

    if padded_quantities:
        signals.append("padded_quantities")

    fraud_detected = (
        "round_number_to_brand_new_vendor" in signals
        and (
            "no_line_item_detail" in signals
            or "off_hours" in signals
            or "padded_quantities" in signals
        )
    )

    return {
        "detected": fraud_detected,
        "signals": signals,
    }
    
    
    

# Return whether a given invoice has instructions which try to influence the decision of the system improperly.
def detect_payload_instructions(invoice: dict) -> bool:

    text_parts = []
    
    for item in invoice.get("lineItems", []):
        if isinstance(item, dict):
            text_parts.append(str(item.get("description", "")))
            
    text_parts.append(str(invoice.get("notes", "")))

    combined_text = " ".join(text_parts).lower()

    suspicious_phrases = [
        "approve me",
        "no need to review",
        "finance already ok",
        "finance already ok'd",
        "already approved",
        "do not escalate",
        "ignore the policy",
        "override policy",
    ]

    found_suspicious_phrase = False
    
    for phrase in suspicious_phrases:
        if phrase in combined_text:
            found_suspicious_phrase = True
            break
        
    return found_suspicious_phrase
    
    
    

# Check global rules and return a corresponding result for a given invoice.     
def check_global_rules(invoice: dict) -> dict:

    hard_stops = []

    line_items_sum = 0
    for item in invoice["lineItems"]:
        line_items_sum += item["quantity"] * item["unitPrice"]

    calculated_total = line_items_sum + invoice["taxAmount"]
    declared_total = invoice["total"]
    math_valid = round(calculated_total, 2) == round(declared_total, 2)
    
    if not math_valid:
        hard_stops.append("GLOBAL-MATH")

    currency = invoice["currency"]
    fx_rate = FX_RATES[currency]
    amount_usd = round(declared_total * fx_rate, 2)

    if currency != "USD":
        if amount_usd > AUTONOMY_CEILING or amount_usd > 1000:
            hard_stops.append("GLOBAL-FX")

    receipt_required = amount_usd > 25
    receipt_present = invoice["receiptPresent"]

    if receipt_required and not receipt_present:
        hard_stops.append("GLOBAL-RECEIPT")

    vendor_known = invoice["vendorKnown"]

    if not vendor_known:
        hard_stops.append("GLOBAL-VENDOR")

    signature = build_duplicate_signature(invoice)
    duplicate = signature in PROCESSED_INVOICE_SIGNATURES

    if duplicate:
        hard_stops.append("GLOBAL-DUP")

    fraud_result = detect_fraud_signals(invoice, amount_usd)
    fraud_signal = fraud_result["detected"]
    
    if fraud_signal:
        hard_stops.append("GLOBAL-FRAUD")

    return {
        "mathValid": math_valid,
        "lineItemsSum": round(line_items_sum, 2),
        "taxAmount": invoice["taxAmount"],
        "calculatedTotal": round(calculated_total, 2),
        "declaredTotal": declared_total,
        "currency": currency,
        "fxRate": fx_rate,
        "amountUsd": amount_usd,
        "receiptRequired": receipt_required,
        "receiptPresent": receipt_present,
        "vendorKnown": vendor_known,
        "duplicate": duplicate,
        "signature": signature,
        "fraudSignal": fraud_signal,
        "fraudSignals": fraud_result["signals"],
        "payloadInstructionSignal": detect_payload_instructions(invoice),
        "hardStops": hard_stops,
    }




# Return the list for the IDs of the rules violated by a given classification made by an AI agent.
def policy_violation_rule_ids(classification: dict) -> list[str]:
    
    rule_ids = []

    for violation in classification.get("policyViolations", []):
        rule_ids.append(violation["rule_id"])

    return rule_ids




# Try to validate and normalize a given runtime policy configuration.
# Raise an error if occurs.
def normalize_runtime_config(config: dict) -> dict:

    if not isinstance(config, dict):
        raise ValueError("config must be a JSON object")

    autonomy_ceiling = config.get(
        "autonomyCeiling",
        DEFAULT_RUNTIME_CONFIG["autonomyCeiling"],
    )

    autonomy_confidence = config.get(
        "autonomyConfidence",
        DEFAULT_RUNTIME_CONFIG["autonomyConfidence"],
    )

    try:
        autonomy_ceiling = float(autonomy_ceiling)
    except (TypeError, ValueError) as error:
        raise ValueError("autonomyCeiling must be a number") from error

    try:
        autonomy_confidence = float(autonomy_confidence)
    except (TypeError, ValueError) as error:
        raise ValueError("autonomyConfidence must be a number") from error

    if autonomy_ceiling < 0:
        raise ValueError("autonomyCeiling must be zero or greater")

    if autonomy_confidence < 0 or autonomy_confidence > 1:
        raise ValueError("autonomyConfidence must be between 0 and 1")

    return {
        "autonomyCeiling": autonomy_ceiling,
        "autonomyConfidence": autonomy_confidence,
    }




# Try to load the current runtime configuration from the Dapr state store.
# Raise an error if occurs.
def load_runtime_config() -> dict:

    try:
        saved_config = dapr_get_state(
            key = RUNTIME_CONFIG_STATE_KEY,
            default = None,
        )
    except DaprClientError:
        saved_config = None

    if saved_config is None:
        return dict(DEFAULT_RUNTIME_CONFIG)

    return normalize_runtime_config(saved_config)
