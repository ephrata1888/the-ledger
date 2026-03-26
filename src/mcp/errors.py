"""Serialize domain and infrastructure errors to MCP-safe typed payloads."""

from __future__ import annotations

from typing import Any

from src.models.events import DomainError, OptimisticConcurrencyError


def _suggested_action_for_domain(exc: DomainError) -> str:
    """Stable hints for MCP clients when a DomainError is returned."""
    code = exc.error_code
    if code == "APPLICATION_ALREADY_EXISTS":
        return "use_a_new_application_id_or_verify_empty_loan_stream"
    if code == "RULE5_COMPLIANCE_GATE":
        return "complete_compliance_rule_passes_before_application_approved"
    if code in ("INVALID_STATE_FOR_EVENT", "INVALID_STATE_TRANSITION"):
        return "verify_loan_state_and_event_order_then_retry"
    if code == "RULE6_CAUSAL_CHAIN":
        return "ensure_contributing_agent_sessions_include_credit_or_fraud_for_application"
    return "review_error_code_and_metadata"


def serialize_exception(exc: BaseException) -> dict[str, Any]:
    """Return a JSON-serializable dict with a mandatory ``error_type`` key."""
    if isinstance(exc, OptimisticConcurrencyError):
        return {
            "error_type": "OptimisticConcurrencyError",
            "stream_id": exc.stream_id,
            "expected_version": exc.expected_version,
            "actual_version": exc.actual_version,
            "suggested_action": "reload_stream_and_retry",
        }
    if isinstance(exc, DomainError):
        return {
            "error_type": "DomainError",
            "error_code": exc.error_code,
            "message": str(exc),
            "metadata": exc.metadata,
            "suggested_action": _suggested_action_for_domain(exc),
        }
    return {
        "error_type": "InternalError",
        "message": str(exc),
        "suggested_action": "check_server_logs",
    }
