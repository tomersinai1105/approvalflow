# Architecture Decision Records

This file documents the main architecture decisions made in the ApprovalFlow project.

Each ADR follows the structure:

- Context
- Decision
- Consequences

# ADR 0001: Use Dapr for service communication, state, pub/sub, and secrets

## Context

ApprovalFlow is built as a microservice-based system. The services need to communicate with each other, publish asynchronous events, store workflow state, and access secrets such as the LLM API key.

The system also needs to avoid tight coupling between services and infrastructure details.

## Decision

The system uses Dapr as the main application runtime for service-to-service communication, Pub/Sub, state management, and secrets.

Dapr service invocation is used for synchronous calls between services, such as:

- API Gateway to Invoice Service
- Workflow Service to Decision Service
- Workflow Service to Payment Service
- Workflow Service to Audit Service
- API Gateway to Query Service

Dapr Pub/Sub is used for asynchronous events:

- `invoice.submitted`
- `workflow.finished`

Dapr state is used for workflow state, human review tasks, processed invoice signatures, payment saga state, audit records, and dashboard data.

Dapr local secret store is used for local secrets such as the LLM API key.

## Consequences

This keeps the services loosely coupled and lets each service focus on its own business logic.

It also makes the architecture easier to change because services do not need to know the internal network location of other services.

The trade-off is that the system depends on Dapr sidecars and Dapr components, so the Docker Compose setup is more complex than a simple monolith.

# ADR 0001: Use Dapr for service communication, state, pub/sub, and secrets

## Context

ApprovalFlow is built as a microservice-based system. The services need to communicate with each other, publish asynchronous events, store workflow state, and access secrets such as the LLM API key.

The system also needs to avoid tight coupling between services and infrastructure details.

## Decision

The system uses Dapr as the main application runtime for service-to-service communication, Pub/Sub, state management, and secrets.

Dapr service invocation is used for synchronous calls between services, such as:

- API Gateway to Invoice Service
- Workflow Service to Decision Service
- Workflow Service to Payment Service
- Workflow Service to Audit Service
- API Gateway to Query Service

Dapr Pub/Sub is used for asynchronous events:

- `invoice.submitted`
- `workflow.finished`

Dapr state is used for workflow state, human review tasks, processed invoice signatures, payment saga state, audit records, and dashboard data.

Dapr local secret store is used for local secrets such as the LLM API key.

## Consequences

This keeps the services loosely coupled and lets each service focus on its own business logic.

It also makes the architecture easier to change because services do not need to know the internal network location of other services.

The trade-off is that the system depends on Dapr sidecars and Dapr components, so the Docker Compose setup is more complex than a simple monolith.

# ADR 0003: Use a payment saga for budget reservation and payment execution

## Context

Approved invoices require a payment flow. Payment is not a single safe operation because the system must reserve budget, execute payment, and then commit the budget reservation.

If payment fails after the budget was reserved, the system must release the reservation. Otherwise, the budget could remain stuck.

The system must avoid partial payments, orphaned budget reservations, and double payments.

## Decision

The Payment Service implements the payment flow as a saga.

The saga steps are:

1. Check whether a payment saga already exists.
2. Reserve department budget.
3. Execute payment.
4. If payment succeeds, commit the budget reservation.
5. If payment fails, release the budget reservation.
6. Save the final saga status.

## Consequences

The payment flow has a consistent outcome.

Successful payments end with:

```text
paymentStatus: paid
reservationStatus: committed
budgetCommitmentStatus: committed
```

Failed payments end with compensation:

```text
paymentStatus: failed
reservationStatus: released
compensationStatus: released
```

This prevents orphaned reservations and supports safe retries.

The trade-off is that the payment logic is more complex because each step must be stored and handled carefully. However, this complexity is necessary because payment and budget updates must remain consistent across services.

# ADR 0004: Use Dapr state for idempotency and durable human review

## Context

The system must handle duplicate submissions, redelivered Pub/Sub events, and retried payments without creating duplicate effects.

The system also needs to pause invoices for human review and resume them later, even if a service restarts.

If this information is kept only in memory, it can be lost when a service restarts. That would make duplicate detection, workflow resume, and payment safety unreliable.

## Decision

The system uses Dapr state to persist important workflow records.

The following data is saved in Dapr state:

1. Gateway submission records.
2. Gateway status records.
3. Workflow state.
4. Human review tasks.
5. Processed invoice signatures.
6. Payment saga records.
7. Payment records.
8. Budget reservation records.
9. Audit records.

Duplicate invoice detection is based on a stable invoice signature.

Human review tasks are saved by correlation ID and can be loaded later when the approver chooses one of the available actions:

```text
approve
reject
request_info
```

## Consequences

The workflow can survive service restarts because important state is not kept only in memory.

Duplicate submissions and retried events are safer because the system checks existing state before creating new effects.

This helps prevent:

```text
duplicate payments
duplicate workflow effects
lost human review tasks
lost payment saga status
```

The trade-off is that services must carefully manage state keys and state ownership. If state keys are not consistent, different services may read or write the wrong records.

However, this trade-off is acceptable because durable state is necessary for idempotency, human review pause/resume, and payment consistency.

# ADR 0005: Make autonomy thresholds externally configurable

## Context

The controller needs to change the policy and autonomy thresholds without redeploying the code.

The autonomy ceiling and confidence threshold are important safety settings. They control when the system is allowed to auto-approve an invoice and when it must escalate the invoice to human review.

If these values are hard-coded, every policy change would require a code change, rebuild, and redeployment.

## Decision

The system stores runtime policy configuration in Dapr state.

The controller can update the configuration through the API Gateway.

The main configuration values are:

```json
{
  "autonomyCeiling": 250,
  "autonomyConfidence": 0.8
}
```

The Workflow Service loads the current configuration through the Decision Service before the deterministic router makes a final decision.

## Consequences

The controller can change the autonomy posture without changing code or rebuilding containers.

This supports safer operations because the system can become more conservative or more permissive through configuration.

For example, lowering the autonomy ceiling causes more invoices to go to human review. Raising it allows more low-risk invoices to be handled automatically, as long as the deterministic safety checks still pass.

The trade-off is that the router must always load and validate the current configuration before using it.

However, this trade-off is acceptable because external configuration is required for safe controller control and for changing thresholds without redeployment.

# ADR 0006: Keep audit as a separate service

## Context

The system must provide a complete decision trail for every invoice.

The audit trail needs to include the invoice data, rules applied, AI reasoning, router decision, final decision maker, human action if there was one, and payment outcome.

This information should be stored clearly and separately from the temporary workflow execution logic.

## Decision

The system uses a dedicated Audit Service.

The Workflow Service calls the Audit Service when a final result is created.

The Audit Service stores audit records by correlation ID and by tracking ID.

The audit record includes:

1. Invoice data.
2. Preprocess results.
3. Applied rules.
4. AI recommendation and reasoning.
5. Router decision.
6. Final decision maker.
7. Human review action, if relevant.
8. Payment saga outcome.

## Consequences

Audit records are separated from the workflow execution logic.

This makes the system easier to inspect, debug, and explain during evaluation.

It also allows an auditor to retrieve the full decision trail for a specific invoice using one correlation ID or tracking ID.

The trade-off is that the Workflow Service depends on another service when saving the final result.

However, this trade-off is acceptable because auditability is a core requirement of the system.

# ADR 0007: Use an API Gateway as the single external entry point

## Context

The system has several internal microservices, but external clients should not call each service directly.

The UI and users need one clear entry point for submitting invoices, checking status, viewing the human review queue, updating controller configurationapr service invocation and Dapr Pub/Sub.

The API Gateway handles:

1. Invoice submission.
2. Immediate acknowledgement with tracking ID.
3. Status lookup.
4. SSE notification endpoint.
5. Human review queue endpoints.
6. Controller dashboard endpoints.
7. Runtime policy configuration endpoints.
8. Audit lookup endpoints.

The API Gateway does not own the business approval decision. The final approval route is handled by the Workflow Service and the deterministic router.

## Consequences

External access is simpler and more controlled.

The UI only needs to communicate with one service instead of calling every microservice directly.

This also makes the system easier to secure, monitor, and extend because external traffic passes through one gateway.

The trade-off is that the API Gateway becomes an important coordination point for external requests.

However, this trade-off is acceptable because the gateway stays focused on external access, request routing, status exposure, and notifications, while business decisions remain inside the workflow and decision services.