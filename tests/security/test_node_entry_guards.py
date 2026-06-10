"""Domain Verification Suite: Node Entry Guards.

This suite exercises the fine-grained capability firewalls, role claim verifications,
and fail-closed perimeter protections enforced around graph canvas node boundaries.
"""

import uuid
from typing import Any

import pytest
from langgraph.checkpoint.memory import MemorySaver
from langgraph.graph import END, START, StateGraph

from langgraph_gatekeeper import (
    GatekeeperMessagesState,
    SecureWorkflowGateway,
    execute_graph,
)


def test_initial_execution_clears_gate_with_correct_role(secure_guard_graph):
    """SCENARIO: Verifies that a user profile carrying the mandatory permission claim token

    clears the boundary check and advances seamlessly to the target node's frozen state.
    """
    config = {
        "configurable": {
            "thread_id": f"t_{uuid.uuid4()}",
            "user_id": "derek",
            "user_claims": ["assign_analyst"],
        }
    }
    list(execute_graph(secure_guard_graph, {}, config))
    assert secure_guard_graph.get_state(config).next == ("functional_gate",)


def test_initial_execution_fails_with_unauthorized_role(secure_guard_graph):
    """SCENARIO: Verifies that a user profile missing the required permission claim token

    is forcefully blocked at the perimeter with a hard PermissionError exception.
    """
    config = {
        "configurable": {
            "thread_id": f"t_{uuid.uuid4()}",
            "user_id": "malicious_actor",
            "user_claims": ["unverified_guest"],
        }
    }
    with pytest.raises(PermissionError) as exc_info:
        list(execute_graph(secure_guard_graph, {}, config))
    assert "SECURITY INTERCEPTION" in str(exc_info.value)


def test_fail_closed_firewall_blocks_spoof_attempts_with_missing_user_id():
    """SCENARIO: Enforces strict fail-closed boundary protection, verifying that completely

    anonymous, un-authenticated configuration contexts are forcefully blocked by default.
    """

    def restricted_node(state: dict) -> dict:
        return {}

    workflow = StateGraph(GatekeeperMessagesState)
    workflow.add_node("restricted_target", restricted_node)
    workflow.add_edge(START, "restricted_target")
    workflow.add_edge("restricted_target", END)

    gateway = SecureWorkflowGateway()
    gateway.enforce_entry("restricted_target", required_claim="admin_clearance")
    graph = gateway.compile(workflow, checkpointer=MemorySaver())

    malicious_config = {"configurable": {"thread_id": f"t_attack_{uuid.uuid4()}"}}

    with pytest.raises(PermissionError) as exc_info:
        list(execute_graph(graph, {}, malicious_config))
    assert "SECURITY INTERCEPTION" in str(exc_info.value)


def test_resumption_action_fails_closed_on_cache_miss():
    """SECURITY ASSURANCE LOCK: Verifies that a 'resume' action with a cache-miss

    in the task ledger database is forcefully caught and blocked by the firewall.
    """
    from langgraph.checkpoint.memory import MemorySaver
    from langgraph.graph import END, START, StateGraph

    from langgraph_gatekeeper import (
        GatekeeperMessagesState,
        SecureWorkflowGateway,
    )

    def secure_target_node(state: Any) -> dict:
        return {"messages": ["CRITICAL EXPEDITION EXECUTED WITHOUT AUTHORIZATION"]}

    workflow = StateGraph(GatekeeperMessagesState)
    workflow.add_node("restricted_compliance_node", secure_target_node)
    workflow.add_edge(START, "restricted_compliance_node")
    workflow.add_edge("restricted_compliance_node", END)

    gateway = SecureWorkflowGateway()
    gateway.enforce_entry(
        "restricted_compliance_node", required_claim="admin_clearance"
    )

    compiled_graph = gateway.compile(workflow, checkpointer=MemorySaver())

    # Trigger a resumption turn pass with a deliberate cache-miss context
    malicious_actor_config = {
        "configurable": {
            "thread_id": "t_exploit_resume_456",
            "user_id": "malicious_actor",
            "user_claims": ["unverified_guest"],
            "active_action": "resume",
            "active_business_context": "stale_or_missing_context",
        }
    }

    # FIXED: The system now correctly fails-closed. The exception is successfully raised!
    with pytest.raises(PermissionError) as exc_info:
        for event in compiled_graph.stream(
            {"messages": []}, config=malicious_actor_config
        ):
            if not event:
                break

    # Proves the dynamic fail-closed ledger check successfully blocked the bypass attempt
    assert "No active transaction token found for business context" in str(
        exc_info.value
    )
    assert "restricted_compliance_node" in str(exc_info.value)
