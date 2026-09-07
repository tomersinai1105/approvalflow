# Verification by running journeys and anti-cheese rules. 



import os
from io import StringIO

import asyncio
import copy
import inspect
import json

import sys
from contextlib import redirect_stderr, redirect_stdout

from pathlib import Path
from typing import Any




PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from app.microservices.workflow_service import workflow



SAMPLE_FILE_CANDIDATES = [
    PROJECT_ROOT / "config" / "sample-invoices.json",
    PROJECT_ROOT / "sample-invoices.json",
]

SHOW_WORKFLOW_LOGS = os.getenv("VERIFY_SHOW_WORKFLOW_LOGS", "0") == "1"




async def maybe_await(value):
    if inspect.isawaitable(value):
        return await value
    return value




def reset_local_runtime_state() -> None:
    
    workflow_state = getattr(workflow, "WORKFLOW_STATE", None)
    if workflow_state is not None and hasattr(workflow_state, "clear"):
        workflow_state.clear()

    human_review_tasks = getattr(workflow, "HUMAN_REVIEW_TASKS", None)
    if human_review_tasks is not None and hasattr(human_review_tasks, "clear"):
        human_review_tasks.clear()




def find_sample_file() -> Path:
    
    for path in SAMPLE_FILE_CANDIDATES:
        if path.exists():
            return path

    raise FileNotFoundError(
        "Could not find sample-invoices.json under config/ or project root."
    )




def load_fixtures() -> dict[str, dict]:
    sample_file = find_sample_file()

    with open(sample_file, "r", encoding="utf-8") as file:
        data = json.load(file)

    fixtures = {}

    for fixture in data.get("fixtures", []):
        
        fixture_id = fixture.get("id")
        if not fixture_id:
            continue

        invoice = copy.deepcopy(fixture)
        
        invoice.pop("expected", None)

        fixtures[fixture_id] = invoice

    return fixtures




def contains_text(value: Any, text: str) -> bool:
    
    text = text.lower()

    if isinstance(value, str):
        return text in value.lower()

    if isinstance(value, dict):
        return any(contains_text(item, text) for item in value.values())

    if isinstance(value, list):
        return any(contains_text(item, text) for item in value)

    return False




async def run_invoice(invoice: dict) -> dict:
    
    async def call_workflow():
        
        outputs = await maybe_await(workflow.run_invoice_workflow(invoice))

        if not outputs:
            return {
                "status": "workflow_error",
                "finalRoute": "workflow_error",
                "message": "Workflow returned no output.",
            }

        if isinstance(outputs, list):
            return outputs[-1]

        return outputs

    if SHOW_WORKFLOW_LOGS:
        return await call_workflow()

    buffer = StringIO()
    with redirect_stdout(buffer), redirect_stderr(buffer):
        return await call_workflow()




async def approve_human_review(result: dict, approver: str) -> dict:
    
    data = normalize(result)
    correlation_id = data.get("correlationId") or data.get("trackingId")

    if not correlation_id:
        return {
            "status": "verification_error",
            "finalRoute": "verification_error",
            "message": "Missing correlationId/trackingId.",
        }

    async def call_approval():
        return await maybe_await(
            workflow.resolve_human_review(
                correlation_id=correlation_id,
                action="approve",
                approver=approver,
                comment="Approved by D5 verification.",
            )
        )

    if SHOW_WORKFLOW_LOGS:
        return await call_approval()

    buffer = StringIO()
    with redirect_stdout(buffer), redirect_stderr(buffer):
        return await call_approval()




def normalize(result: Any) -> dict:
    
    if isinstance(result, list):
        if not result:
            return {}
        result = result[-1]

    if not isinstance(result, dict):
        return {}

    merged = {}

    workflow_outputs = result.get("workflowOutputs")
    if isinstance(workflow_outputs, list) and workflow_outputs:
        if isinstance(workflow_outputs[-1], dict):
            merged.update(workflow_outputs[-1])

    nested_result = result.get("result")
    if isinstance(nested_result, dict):
        merged.update(nested_result)

    for key, value in result.items():
        if key not in ("workflowOutputs", "result"):
            merged[key] = value

    return merged




def get_preprocess(result: Any) -> dict:
    data = normalize(result)

    preprocess = data.get("preprocess")
    if isinstance(preprocess, dict):
        return preprocess

    audit_trail = data.get("auditTrail")
    if isinstance(audit_trail, dict):
        router_response = audit_trail.get("routerResponse")
        if isinstance(router_response, dict):
            preprocess = router_response.get("preprocess")
            if isinstance(preprocess, dict):
                return preprocess

    return {}




def get_rules(result: Any) -> list:
    data = normalize(result)

    applied_rules = data.get("appliedRules")
    if isinstance(applied_rules, list):
        return applied_rules

    audit_trail = data.get("auditTrail")
    if isinstance(audit_trail, dict):
        router_response = audit_trail.get("routerResponse")
        if isinstance(router_response, dict):
            applied_rules = router_response.get("appliedRules")
            if isinstance(applied_rules, list):
                return applied_rules

    return []




def get_payment_saga(result: Any) -> dict:
    data = normalize(result)

    saga = data.get("paymentSaga")
    if isinstance(saga, dict):
        return saga

    audit_trail = data.get("auditTrail")
    if isinstance(audit_trail, dict):
        payment_outcome = audit_trail.get("paymentOutcome")
        if isinstance(payment_outcome, dict):
            return payment_outcome

    return {}




def get_payment_status(result: Any) -> str | None:
    saga = get_payment_saga(result)

    payment = saga.get("payment")
    if isinstance(payment, dict):
        return payment.get("status")

    return saga.get("paymentStatus") or saga.get("status")




def get_reservation_status(result: Any) -> str | None:
    saga = get_payment_saga(result)

    reservation = saga.get("reservation")
    if isinstance(reservation, dict):
        return reservation.get("status")

    return saga.get("reservationStatus")




def get_budget_commitment_status(result: Any) -> str | None:
    saga = get_payment_saga(result)

    commitment = saga.get("budgetCommitment")
    if isinstance(commitment, dict):
        return commitment.get("status")

    return saga.get("budgetCommitmentStatus")




def get_compensation_status(result: Any) -> str | None:
    saga = get_payment_saga(result)

    compensation = saga.get("compensation")
    if isinstance(compensation, dict):
        return compensation.get("status")

    return saga.get("compensationStatus")




def compact_result(result: Any) -> dict:
    
    data = normalize(result)
    preprocess = get_preprocess(result)
    saga = get_payment_saga(result)

    return {
        "trackingId": data.get("trackingId"),
        "correlationId": data.get("correlationId"),
        "status": data.get("status"),
        "finalRoute": data.get("finalRoute"),
        "message": data.get("message"),
        "rules": get_rules(result),
        "duplicate": preprocess.get("duplicate"),
        "hardStops": preprocess.get("hardStops"),
        "payloadInstructionSignal": preprocess.get("payloadInstructionSignal"),
        "paymentSagaStatus": saga.get("status"),
        "paymentStatus": get_payment_status(result),
        "reservationStatus": get_reservation_status(result),
        "budgetCommitmentStatus": get_budget_commitment_status(result),
        "compensationStatus": get_compensation_status(result),
    }




def is_auto_approved_without_human(result: Any) -> bool:
    data = normalize(result)
    saga = get_payment_saga(result)

    return (
        data.get("finalRoute") == "auto_approve"
        and data.get("status") == "paid"
        and saga.get("status") == "paid"
        and data.get("humanReviewTask") in (None, {})
    )




def is_waiting_for_human_review(result: Any) -> bool:
    data = normalize(result)

    return (
        data.get("finalRoute") == "human_review"
        and data.get("status") == "waiting_for_human_review"
    )




def is_human_approved_paid(result: Any) -> bool:
    data = normalize(result)
    saga = get_payment_saga(result)

    return (
        data.get("finalRoute") == "human_approved"
        and data.get("status") == "paid"
        and saga.get("status") == "paid"
        and (
            get_reservation_status(result) == "committed"
            or get_budget_commitment_status(result) == "committed"
        )
    )




def is_duplicate_without_payment(result: Any) -> bool:
    data = normalize(result)
    preprocess = get_preprocess(result)
    saga = get_payment_saga(result)

    return (
        data.get("finalRoute") == "duplicate"
        and data.get("status") == "duplicate_no_payment"
        and preprocess.get("duplicate") is True
        and "GLOBAL-DUP" in get_rules(result)
        and not saga
    )




def is_payment_failed_compensated(result: Any) -> bool:
    data = normalize(result)
    saga = get_payment_saga(result)

    return (
        data.get("status") == "payment_failed"
        and saga.get("status") == "payment_failed"
        and get_payment_status(result) == "failed"
        and get_reservation_status(result) == "released"
        and get_compensation_status(result) == "released"
        and get_budget_commitment_status(result) in (None, "")
    )




def print_check(name: str, passed: bool, expected: str = "", actual: Any = None) -> bool:
    if passed:
        print(f"PASS - {name}")
    else:
        print(f"FAIL - {name}")
        if expected:
            print(f"  expected: {expected}")
        if actual is not None:
            print(f"  actual:   {actual}")

    return passed




def print_result(title: str, result: Any) -> None:
    print(title)
    print(json.dumps(compact_result(result), indent=2, ensure_ascii=False))




# Main D5 verification
async def verify() -> int:
    
    reset_local_runtime_state()

    fixtures = load_fixtures()

    required_ids = ["INV-1001", "INV-1003", "INV-1007", "INV-1012"]

    missing = [invoice_id for invoice_id in required_ids if invoice_id not in fixtures]

    if missing:
        print("D5 FAIL")
        print(f"Missing required D5 fixtures: {missing}")
        return 1

    first_results: dict[str, dict] = {}
    approval_results: dict[str, dict] = {}

    print()
    print("=== ApprovalFlow D5 verification ===")
    print("Mode: real sample-invoices.json fixtures")
    print("This verifier does not use expected.route and does not rewrite invoice data.")
    print("=" * 45)


    # Journey 1: Auto approve — INV-1001

    print()
    print("Running D5 Journey 1: INV-1001 auto-approve")

    result_1001 = await run_invoice(fixtures["INV-1001"])
    first_results["INV-1001"] = result_1001
    print_result("INV-1001 result:", result_1001)


    # Journey 2: Duplicate — INV-1007 after INV-1001

    print()
    print("Running D5 Journey 2: INV-1007 duplicate after INV-1001")

    result_1007 = await run_invoice(fixtures["INV-1007"])
    first_results["INV-1007"] = result_1007
    print_result("INV-1007 result:", result_1007)


    # Journey 3: Escalate and resume — INV-1003

    print()
    print("Running D5 Journey 3: INV-1003 human review then resume")

    result_1003 = await run_invoice(fixtures["INV-1003"])
    first_results["INV-1003"] = result_1003
    print_result("INV-1003 before human approval:", result_1003)

    if is_waiting_for_human_review(result_1003):
        approval_1003 = await approve_human_review(
            result_1003,
            approver="manager@northwind.example",
        )
        approval_results["INV-1003"] = approval_1003
        print_result("INV-1003 after human approval:", approval_1003)


    # Journey 4: Payment failure + compensation — INV-1012

    print()
    print("Running D5 Journey 4: INV-1012 payment failure + compensation")

    result_1012 = await run_invoice(fixtures["INV-1012"])
    first_results["INV-1012"] = result_1012
    print_result("INV-1012 before human approval:", result_1012)

    if is_waiting_for_human_review(result_1012):
        approval_1012 = await approve_human_review(
            result_1012,
            approver="finance.manager@northwind.example",
        )
        approval_results["INV-1012"] = approval_1012
        print_result("INV-1012 after human approval:", approval_1012)


    # Run every remaining fixture for anti-cheese/evidence

    print()
    print("Running remaining sample fixtures for anti-cheese coverage")

    already_run = set(first_results.keys())

    for fixture_id, invoice in fixtures.items():
        if fixture_id in already_run:
            continue

        print()
        print(f"Running {fixture_id}")

        result = await run_invoice(invoice)
        first_results[fixture_id] = result
        print_result(f"{fixture_id} result:", result)


    # D5 checks
    
    print()
    print("=== D5 checks ===")

    checks = []

    checks.append(
        print_check(
            "Journey 1 - INV-1001 auto-approves with no human",
            is_auto_approved_without_human(first_results.get("INV-1001")),
            expected="finalRoute=auto_approve, status=paid, paymentSaga=paid, no humanReviewTask",
            actual=compact_result(first_results.get("INV-1001")),
        )
    )

    checks.append(
        print_check(
            "Journey 2 - INV-1007 duplicate is short-circuited with no second payment",
            is_duplicate_without_payment(first_results.get("INV-1007")),
            expected="finalRoute=duplicate, status=duplicate_no_payment, GLOBAL-DUP, no paymentSaga",
            actual=compact_result(first_results.get("INV-1007")),
        )
    )

    checks.append(
        print_check(
            "Journey 3 - INV-1003 pauses for human review and resumes to paid",
            (
                is_waiting_for_human_review(first_results.get("INV-1003"))
                and is_human_approved_paid(approval_results.get("INV-1003"))
            ),
            expected="before=human_review/waiting_for_human_review, after=human_approved/paid",
            actual={
                "before": compact_result(first_results.get("INV-1003")),
                "after": compact_result(approval_results.get("INV-1003")),
            },
        )
    )

    checks.append(
        print_check(
            "Journey 4 - INV-1012 payment failure is compensated",
            (
                is_waiting_for_human_review(first_results.get("INV-1012"))
                and is_payment_failed_compensated(approval_results.get("INV-1012"))
            ),
            expected="payment_failed, payment=failed, reservation=released, compensation=released",
            actual={
                "before": compact_result(first_results.get("INV-1012")),
                "after": compact_result(approval_results.get("INV-1012")),
            },
        )
    )

    auto_approved_ids = [
        fixture_id
        for fixture_id, result in first_results.items()
        if is_auto_approved_without_human(result)
    ]

    checks.append(
        print_check(
            "Anti-cheese - at least 2 items auto-approve with no human",
            len(auto_approved_ids) >= 2,
            expected="at least 2 auto-approved items",
            actual={
                "count": len(auto_approved_ids),
                "ids": auto_approved_ids,
            },
        )
    )

    approve_me_ids = [
        fixture_id
        for fixture_id, invoice in fixtures.items()
        if contains_text(invoice, "approve me")
    ]

    approve_me_results = {
        fixture_id: first_results.get(fixture_id)
        for fixture_id in approve_me_ids
    }

    approve_me_passed = (
        bool(approve_me_ids)
        and all(
            result is not None
            and normalize(result).get("finalRoute") != "auto_approve"
            for result in approve_me_results.values()
        )
    )

    checks.append(
        print_check(
            'Anti-cheese - "approve me" payload does not flip decision to auto-approve',
            approve_me_passed,
            expected='fixture containing "approve me" must not finalRoute=auto_approve',
            actual={
                fixture_id: compact_result(result)
                for fixture_id, result in approve_me_results.items()
            },
        )
    )

    print()
    print("=== D5 summary ===")
    print(f"Fixtures run: {len(first_results)}/{len(fixtures)}")
    print(f"Auto-approved ids: {auto_approved_ids}")
    print(f"Approve-me ids: {approve_me_ids}")
    print(f"Checks passed: {sum(1 for check in checks if check)}/{len(checks)}")

    if all(checks):
        print("ALL VERIFICATION CHECKS PASSED")
        print("D5 PASS")
        return 0

    print("SOME VERIFICATION CHECKS FAILED")
    print("D5 FAIL")
    return 1


if __name__ == "__main__":
    exit_code = asyncio.run(verify())
    raise SystemExit(exit_code)