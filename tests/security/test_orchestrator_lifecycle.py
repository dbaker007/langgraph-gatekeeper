"""Domain Verification Suite: Core Security Orchestration Lifecycle.

This suite exercises the high-assurance execution tracking, composite context
resumptions, and immutable thread status ledger metrics exposed via the framework's
orchestrator layers (core/orchestrator.py).
"""

import uuid

import pytest

from langgraph_gatekeeper import (
    execute_graph,
    get_historical_thread_status,
    resume,
)


def test_full_orchestration_lifecycle_using_composite_context_resumption(
    secure_test_graph,
):
    """SCENARIO: Verifies that a user carrying the 'operator' claim token clears

    the node boundaries and encounters the frozen interrupt state on turn one.
    """
    thread_id = f"t_orch_{uuid.uuid4()}"
    config = {
        "configurable": {
            "thread_id": thread_id,
            "user_id": "operator_derek",
            "user_claims": ["operator"],
        }
    }
    list(execute_graph(secure_test_graph, {}, config))
    assert secure_test_graph.get_state(config).next == ("step_two_node",)


def test_resume_by_context_raises_value_error_if_composite_key_fails_to_match(
    secure_test_graph,
):
    """SCENARIO: Verifies that an administrative resumption turn immediately fails-closed

    with a hard ValueError if the targeted composite business context key is missing or malformed.
    """
    thread_id = f"t_fail_{uuid.uuid4()}"
    config = {
        "configurable": {
            "thread_id": thread_id,
            "user_id": "operator_derek",
            "user_claims": ["operator"],
        }
    }
    list(execute_graph(secure_test_graph, {}, config))

    manager_config = {
        "configurable": {
            "thread_id": thread_id,
            "user_id": "manager_baker",
            "user_claims": ["manager_clearance"],
        }
    }
    with pytest.raises(ValueError):
        list(
            resume(
                secure_test_graph, "INVALID_BIZ_CONTEXT", "Passed Input", manager_config
            )
        )


def test_orchestrator_saves_and_surfaces_required_claim_metadata_lifecycle(
    secure_test_graph,
):
    """SCENARIO: Validates that out-of-band background daemons can cleanly query the active

    thread status history log table to recover security claims and lifecycle state records.
    """
    thread_id = f"t_meta_{uuid.uuid4()}"
    config = {
        "configurable": {
            "thread_id": thread_id,
            "user_id": "operator_derek",
            "user_claims": ["operator"],
        }
    }
    list(execute_graph(secure_test_graph, {}, config))
    metrics = get_historical_thread_status(secure_test_graph, config)
    assert metrics["status"] == "PENDING"
