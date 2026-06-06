import sqlite3
import uuid

import pytest
from langgraph.checkpoint.memory import MemorySaver
from langgraph.graph import END, START, StateGraph

from langgraph_gatekeeper import SecureWorkflowGateway


def dummy_mutation_node(state: dict) -> dict:
    return {}


# Configure a lightweight local graph layout to isolate the proxy test
gateway = SecureWorkflowGateway()
workflow = StateGraph(dict)
workflow.add_node("dummy_mutation_node", dummy_mutation_node)
workflow.add_edge(START, "dummy_mutation_node")
workflow.add_edge("dummy_mutation_node", END)

mock_secure_graph = gateway.compile(workflow, checkpointer=MemorySaver())


def test_proxy_lock_blocks_unauthorized_state_mutations():
    """TDD FOCUS - TASK 5: Verifies 'update_state' is programmatically protected.

    1. Positive Pass: A system actor holding 'mutate_state' can modify state.
    2. Negative Failure: A user lacking 'mutate_state' is forcefully blocked.
    """
    # 1. SETUP MALICIOUS CONFIG (Lacks 'mutate_state')
    malicious_config = {
        "configurable": {
            "thread_id": f"t_mutation_hack_{uuid.uuid4()}",
            "user_id": "malicious_actor",
            "user_claims": ["assign_analyst"],  # Missing 'mutate_state'!
        }
    }

    # CRITICAL ATTACK VERIFICATION: This MUST raise a PermissionError!
    with pytest.raises(PermissionError) as exc_info:
        mock_secure_graph.update_state(malicious_config, {"result_data": "HACKED"})

    assert "denied administrative state mutation" in str(exc_info.value)


def test_proxy_lock_allows_authorized_state_mutations():
    """Verifies that an administrator or background service actor carrying

    the explicit 'mutate_state' claim can update thread data unhindered.
    """
    thread_id = f"t_mutation_ok_{uuid.uuid4()}"
    authorized_config = {
        "configurable": {
            "thread_id": thread_id,
            "user_id": "trusted_system_service",
            "user_claims": ["mutate_state"],
        }
    }

    # This execution MUST clear the firewall proxy boundary completely
    mock_secure_graph.update_state(authorized_config, {"test_payload": "CLEARED"})

    snapshot = mock_secure_graph.get_state(authorized_config)
    assert snapshot.values.get("test_payload") == "CLEARED"
