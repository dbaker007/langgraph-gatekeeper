"""Domain Verification Suite: Node Entry Guards.

This suite exercises the fine-grained capability firewalls, role claim verifications,
and fail-closed perimeter protections enforced around graph canvas node boundaries.
"""

import uuid

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
