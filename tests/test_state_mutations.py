import uuid

import pytest
from langgraph.checkpoint.memory import MemorySaver
from langgraph.graph import END, START, StateGraph

from langgraph_gatekeeper import GatekeeperState, SecureWorkflowGateway


class MutationTestState(GatekeeperState):
    result_data: str
    test_payload: str


def dummy_mutation_node(state: dict) -> dict:
    return {}


# Configure a lightweight local graph layout to isolate the proxy test
gateway = SecureWorkflowGateway()
workflow = StateGraph(MutationTestState)
workflow.add_node("dummy_mutation_node", dummy_mutation_node)
workflow.add_edge(START, "dummy_mutation_node")
workflow.add_edge("dummy_mutation_node", END)

mock_secure_graph = gateway.compile(workflow, checkpointer=MemorySaver())


def test_proxy_lock_blocks_unauthorized_state_mutations():
    """TDD FOCUS - TASK 5: Verifies 'update_state' is programmatically protected."""
    malicious_config = {
        "configurable": {
            "thread_id": f"t_mutation_hack_{uuid.uuid4()}",
            "user_id": "malicious_actor",
            "user_claims": ["assign_analyst"],  # Missing 'mutate_state'!
        }
    }

    with pytest.raises(PermissionError) as exc_info:
        mock_secure_graph.update_state(malicious_config, {"result_data": "HACKED"})

    assert "denied administrative state mutation" in str(exc_info.value)


def test_proxy_lock_allows_authorized_state_mutations():
    """Verifies that an administrator holding 'mutate_state' can update thread data."""
    thread_id = f"t_mutation_ok_{uuid.uuid4()}"
    authorized_config = {
        "configurable": {
            "thread_id": thread_id,
            "user_id": "trusted_system_service",
            "user_claims": ["mutate_state"],
        }
    }

    mock_secure_graph.update_state(authorized_config, {"test_payload": "CLEARED"})

    snapshot = mock_secure_graph.get_state(authorized_config)
    assert snapshot.values.get("test_payload") == "CLEARED"
