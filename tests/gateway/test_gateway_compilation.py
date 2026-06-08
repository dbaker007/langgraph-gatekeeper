"""Domain Verification Suite: Secure Gateway Compilation Primitives.

This suite exercises the configuration rules, schema inheritance firewalls,
and fluent entry gate policy matrices managed during the compilation phase
by the framework gateway (core/gateway.py) and compiler (core/compiler.py).
"""

from typing import Any

import pytest
from langgraph.checkpoint.memory import MemorySaver
from langgraph.graph import END, START, StateGraph

from langgraph_gatekeeper import (
    GatekeeperMessagesState,
    GatekeeperObjectState,
    SecureWorkflowGateway,
    execute_graph,
)


def test_compiler_accepts_structural_custom_state_but_fails_claims_validation_at_runtime():
    """SCENARIO: Verifies that a clean, non-subclassed custom class is accepted by the compiler

    if it carries the correct identity attributes, but forcefully fails-closed with a hard
    PermissionError at runtime execution if user claims are unverified.
    """

    class CustomStructuralState:
        """A plain class layout carrying the mandatory contract tracking channels."""

        user_id: str
        user_claims: list
        messages: list

    def restricted_node(state: Any) -> dict:
        return {}

    workflow = StateGraph(CustomStructuralState)
    workflow.add_node("protected_target", restricted_node)
    workflow.add_edge(START, "protected_target")
    workflow.add_edge("protected_target", END)

    gateway = SecureWorkflowGateway()
    gateway.enforce_entry("protected_target", required_claim="admin_clearance")
    compiled_graph = gateway.compile(workflow, checkpointer=MemorySaver())

    # The schema is valid, so compilation succeeds beautifully.
    assert compiled_graph is not None

    # Verification: Ensure runtime execution blocks an unauthorized actor carrying invalid claims
    unauthorized_config = {
        "configurable": {
            "thread_id": "t_gate_test_123",
            "user_id": "malicious_actor",
            "user_claims": ["unverified_guest"],  # Lacks 'admin_clearance'!
        }
    }

    with pytest.raises(PermissionError) as exc_info:
        list(execute_graph(compiled_graph, {}, unauthorized_config))

    assert "SECURITY INTERCEPTION" in str(exc_info.value)


def test_compiler_accepts_and_bakes_both_state_paradigms_symmetrically():
    """SCENARIO: Validates that the compiler cleanly builds graph assets using either the dictionary-based

    (GatekeeperMessagesState) or object-oriented (GatekeeperObjectState) baseline layouts.
    """

    def standard_node(state: Any) -> dict:
        return {}

    # 1. Verify Dictionary-Based (TypedDict) Schema Path
    msg_workflow = StateGraph(GatekeeperMessagesState)
    msg_workflow.add_node("node_a", standard_node)
    msg_workflow.add_edge(START, "node_a")
    msg_workflow.add_edge("node_a", END)

    gateway_a = SecureWorkflowGateway()
    compiled_a = gateway_a.compile(msg_workflow, checkpointer=MemorySaver())
    assert compiled_a is not None

    # 2. Verify Object-Oriented (Pydantic BaseModel) Schema Path
    obj_workflow = StateGraph(GatekeeperObjectState)
    obj_workflow.add_node("node_b", standard_node)
    obj_workflow.add_edge(START, "node_b")
    obj_workflow.add_edge("node_b", END)

    gateway_b = SecureWorkflowGateway()
    compiled_b = gateway_b.compile(obj_workflow, checkpointer=MemorySaver())
    assert compiled_b is not None


def test_gateway_fluent_enforce_entry_maps_policies_accurately():
    """SCENARIO: Verifies that policy rules configured via chained fluent methods on the gateway

    are captured and propagate accurately into the final compiled asset tracking matrices.
    """

    def standard_node(state: Any) -> dict:
        return {}

    workflow = StateGraph(GatekeeperMessagesState)
    workflow.add_node("restricted_node_1", standard_node)
    workflow.add_node("restricted_node_2", standard_node)
    workflow.add_edge(START, "restricted_node_1")
    workflow.add_edge("restricted_node_1", "restricted_node_2")
    workflow.add_edge("restricted_node_2", END)

    # Chain fluent rules upfront
    gateway = SecureWorkflowGateway()
    (
        gateway.enforce_entry(
            "restricted_node_1", required_claim="tier_1_analyst"
        ).enforce_entry("restricted_node_2", required_claim="tier_2_supervisor")
    )

    compiled_graph = gateway.compile(workflow, checkpointer=MemorySaver())

    # FIXED PUBLIC API CONTRACT LOOKUP: Target the actual public property directly
    matrix = compiled_graph.policy_matrix

    assert "tier_1_analyst" in matrix.get("restricted_node_1", {}).get("execute", [])
    assert "tier_2_supervisor" in matrix.get("restricted_node_2", {}).get("execute", [])
