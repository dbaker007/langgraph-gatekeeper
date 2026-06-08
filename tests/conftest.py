"""Shared Global Test Fixtures and Zero-Trust Topology Hooks."""

import pytest
from langgraph.checkpoint.memory import MemorySaver
from langgraph.graph import END, START, StateGraph

from langgraph_gatekeeper import (
    GatekeeperMessagesState,
    SecureWorkflowGateway,
    interrupt,
)


class LabOrchState(GatekeeperMessagesState):
    """Unified dictionary-based test state tracking primitive for orchestrator actions."""

    routing_key: str
    result_data: str


class LabGuardState(GatekeeperMessagesState):
    """Unified state primitive for entry guard verification testing."""

    routing_key: str
    result_data: str


class LabMutationState(GatekeeperMessagesState):
    """Unified state primitive for administrative mutation firewall testing."""

    result_data: str
    test_payload: str


class LabTtlState(GatekeeperMessagesState):
    """Unified state primitive for out-of-band SLA eviction testing."""

    routing_key: str
    result_data: str


def step_one_node(state: LabOrchState) -> dict:
    return {}


def step_two_node(state: LabOrchState) -> dict:
    token = interrupt(
        "task_abc", "composite_biz_context", "manager_clearance", {"details": "HOLDING"}
    )
    return {"result_data": token}


def functional_gate_node(state: LabGuardState) -> dict:
    token = interrupt(
        "task_gate_xyz",
        "framework_base_compliance",
        "verify_underwriter",
        {"status": "AWAITING_VERIFICATION"},
    )
    return {"result_data": token}


def dummy_mutation_node(state: LabMutationState) -> dict:
    return {}


def assign_agent_ttl_node(state: LabTtlState) -> dict:
    import uuid

    unique_key = f"task_ttl_concierge_{uuid.uuid4()}"
    response = interrupt(
        unique_key,
        "hazmat_dispatch_compliance",
        "verify_underwriter",
        {"status": "AWAITING_TTL_SIGN_OFF"},
    )
    return {"routing_key": unique_key, "result_data": response}


def kill_switch(state: LabTtlState) -> dict:
    return {}


# =============================================================================
# 1. STATIC GLOBAL COMPILATION INSTANCES (For out-of-band importlib lookups)
# =============================================================================
_gateway_orch = SecureWorkflowGateway()
_gateway_orch.enforce_entry("step_one_node", required_claim="operator").enforce_entry(
    "step_two_node", required_claim="operator"
)
_wf_orch = StateGraph(LabOrchState)
_wf_orch.add_node("step_one_node", step_one_node)
_wf_orch.add_node("step_two_node", step_two_node)
_wf_orch.add_edge(START, "step_one_node")
_wf_orch.add_edge("step_one_node", "step_two_node")
_wf_orch.add_edge("step_two_node", END)
secure_test_graph_instance = _gateway_orch.compile(_wf_orch, checkpointer=MemorySaver())

_gateway_guard = SecureWorkflowGateway()
_gateway_guard.enforce_entry("functional_gate", required_claim="assign_analyst")
_wf_guard = StateGraph(LabGuardState)
_wf_guard.add_node("functional_gate", functional_gate_node)
_wf_guard.add_edge(START, "functional_gate")
_wf_guard.add_edge("functional_gate", END)
secure_guard_graph_instance = _gateway_guard.compile(
    _wf_guard, checkpointer=MemorySaver()
)

_gateway_mutation = SecureWorkflowGateway()
_wf_mutation = StateGraph(LabMutationState)
_wf_mutation.add_node("dummy_mutation_node", dummy_mutation_node)
_wf_mutation.add_edge(START, "dummy_mutation_node")
_wf_mutation.add_edge("dummy_mutation_node", END)
secure_mutation_graph_instance = _gateway_mutation.compile(
    _wf_mutation, checkpointer=MemorySaver()
)

_gateway_ttl = SecureWorkflowGateway()
_gateway_ttl.enforce_entry(
    "assign_agent_ttl", required_claim="assign_analyst"
).enforce_entry("kill_switch", required_claim="infra_eviction_clearance")
_wf_ttl = StateGraph(LabTtlState)
_wf_ttl.add_node("assign_agent_ttl", assign_agent_ttl_node)
_wf_ttl.add_node("kill_switch", kill_switch)
_wf_ttl.add_edge(START, "assign_agent_ttl")
_wf_ttl.add_edge("assign_agent_ttl", END)
_wf_ttl.add_edge("kill_switch", END)

# STATIC INSTANCE ASSET: This allows register_graph_asset to check standard object primitives flatly!
secure_ttl_graph = _gateway_ttl.compile(_wf_ttl, checkpointer=MemorySaver())


# =============================================================================
# 2. PYTEST FIXTURE FAÇADES (For parameter injection into test signatures)
# =============================================================================
@pytest.fixture(scope="module")
def secure_test_graph():
    """Injects the compiled zero-trust orchestration graph as a parameter."""
    return secure_test_graph_instance


@pytest.fixture(scope="module")
def secure_guard_graph():
    """Injects the compiled entry guard graph as a parameter."""
    return secure_guard_graph_instance


@pytest.fixture(scope="module")
def secure_mutation_graph():
    """Injects the compiled state mutation graph as a parameter."""
    return secure_mutation_graph_instance


@pytest.fixture(scope="module")
def secure_ttl_graph_fixture():
    """LOCKED IN: Facade wrapper handing the static secure_ttl_graph down to test functions."""
    return secure_ttl_graph
