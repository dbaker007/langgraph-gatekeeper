import sqlite3
import uuid

import pytest
from langgraph.checkpoint.memory import MemorySaver
from langgraph.checkpoint.sqlite import SqliteSaver
from langgraph.graph import END, START, StateGraph

from langgraph_gatekeeper import GatekeeperState, SecureWorkflowGateway, execute_graph
from langgraph_gatekeeper.core.orchestrator import interrupt
from langgraph_gatekeeper.core.task_cache_db import TASK_CACHE_DB_PATH

MOCK_GATE_DB = "test_gate_checkpoints.db"


class NodeGateState(GatekeeperState):
    routing_key: str
    result_data: str


def functional_gate_node(state: NodeGateState) -> dict:
    response = interrupt(
        state.routing_key,
        "framework_base_compliance",
        "verify_underwriter",
        {"status": "AWAITING_VERIFICATION"},
    )
    return {"result_data": response}


# Initialize and configure our gateway rules
gateway = SecureWorkflowGateway()
gateway.enforce_entry("functional_gate", required_claim="assign_analyst")

# Construct target canvas topology
workflow = StateGraph(NodeGateState)
workflow.add_node("functional_gate", functional_gate_node)
workflow.add_edge(START, "functional_gate")
workflow.add_edge("functional_gate", END)

conn = sqlite3.connect(MOCK_GATE_DB, check_same_thread=False)
checkpointer = SqliteSaver(conn)
mock_secure_graph = gateway.compile(workflow, checkpointer=checkpointer)


@pytest.fixture(autouse=True)
def cleanup_test_environments():
    yield
    with sqlite3.connect(TASK_CACHE_DB_PATH) as db:
        db.execute("DELETE FROM tasks")
        db.commit()


def test_initial_execution_clears_gate_with_correct_role():
    config = {
        "configurable": {
            "thread_id": f"t_{uuid.uuid4()}",
            "user_id": "derek",
            "user_claims": ["assign_analyst"],
        }
    }
    list(execute_graph(mock_secure_graph, {}, config))
    assert mock_secure_graph.get_state(config).next == ("functional_gate",)


def test_initial_execution_fails_with_unauthorized_role():
    config = {
        "configurable": {
            "thread_id": f"t_{uuid.uuid4()}",
            "user_id": "malicious_actor",
            "user_claims": ["unverified_guest"],
        }
    }
    with pytest.raises(PermissionError) as exc_info:
        list(execute_graph(mock_secure_graph, {}, config))
    assert "SECURITY INTERCEPTION" in str(exc_info.value)


def test_fail_closed_firewall_blocks_spoof_attempts_with_missing_user_id():
    """SECURITY SUITE: Enforces fail-closed protection against anonymous hacks."""

    def secure_node(state: dict) -> dict:
        return {"result": "hack_succeeded"}

    local_workflow = StateGraph(GatekeeperState)
    local_workflow.add_node("restricted_target", secure_node)
    local_workflow.add_edge(START, "restricted_target")
    local_workflow.add_edge("restricted_target", END)

    local_gw = SecureWorkflowGateway()
    local_gw.enforce_entry("restricted_target", required_claim="admin_clearance")
    malicious_secure_graph = local_gw.compile(
        local_workflow, checkpointer=MemorySaver()
    )

    malicious_config = {"configurable": {"thread_id": f"t_attack_{uuid.uuid4()}"}}

    with pytest.raises(PermissionError) as exc_info:
        list(execute_graph(malicious_secure_graph, {}, malicious_config))

    assert "SECURITY INTERCEPTION" in str(exc_info.value)


def test_framework_permits_internal_checkpointer_serialization_passes():
    """SECURITY SUITE: Verifies LangGraph's cleanup crew can save state."""

    def functional_node(state: dict) -> dict:
        return {"status": "node_processed"}

    local_workflow = StateGraph(GatekeeperState)
    local_workflow.add_node("functional_gate", functional_node)
    local_workflow.add_edge(START, "functional_gate")
    local_workflow.add_edge("functional_gate", END)

    local_gw = SecureWorkflowGateway()
    local_gw.enforce_entry("functional_gate", required_claim="standard_user")
    functional_secure_graph = local_gw.compile(
        local_workflow, checkpointer=MemorySaver()
    )

    valid_user_config = {
        "configurable": {
            "thread_id": f"t_clean_pass_{uuid.uuid4()}",
            "user_id": "user_derek",
            "user_claims": ["standard_user"],
        }
    }

    list(execute_graph(functional_secure_graph, {}, valid_user_config))

    final_state = functional_secure_graph.get_state(valid_user_config)
    assert final_state.next == ()
