import sqlite3
import uuid

import pytest
from langgraph.checkpoint.sqlite import SqliteSaver
from langgraph.graph import END, START, StateGraph
from pydantic import BaseModel

from langgraph_gatekeeper import (
    compile_graph_with_authorization,
    execute_graph,
    interrupt,
    resume,
)

MOCK_CHECKPOINT_DB = "test_checkpoints.db"


class TestState(BaseModel):
    input_key: str = ""
    result_data: str = ""


def entry_node(state: TestState) -> dict:
    return {}


def interrupt_node(state: TestState) -> dict:
    # FIXED: Use the correct, framework-compliant 'routing_key' attribute name
    response = interrupt({"routing_key": state.input_key})
    return {"result_data": response}


FRAMEWORK_POLICIES = {
    "entry_node": {"execute": "basic_analyst"},
    "interrupt_node": {"execute": "basic_analyst", "approve": "executive_underwriter"},
}

workflow = StateGraph(TestState)
workflow.add_node("entry_node", entry_node)
workflow.add_node("interrupt_node", interrupt_node)

workflow.add_edge(START, "entry_node")
workflow.add_edge("entry_node", "interrupt_node")
workflow.add_edge("interrupt_node", END)

conn = sqlite3.connect(MOCK_CHECKPOINT_DB, check_same_thread=False)
checkpointer = SqliteSaver(conn)

graph = compile_graph_with_authorization(
    workflow, checkpointer=checkpointer, policy_provider=FRAMEWORK_POLICIES
)


def test_full_orchestration_and_negative_failure_modes():
    thread_id = f"t_frame_{uuid.uuid4()}"
    config = {
        "configurable": {
            "thread_id": thread_id,
            "user_id": "derek",
            "user_claims": ["basic_analyst"],
        }
    }
    list(execute_graph(graph, {"input_key": "TX_777"}, config))
    assert graph.get_state(config).next == ("interrupt_node",)


def test_resumption_retries_after_unauthorized_interception():
    """Validates that a failed unauthorized resumption pass does not destroy the active token cache row."""
    thread_id = f"t_retry_{uuid.uuid4()}"
    routing_token = f"TOKEN_{uuid.uuid4()}"

    analyst_config = {
        "configurable": {
            "thread_id": thread_id,
            "user_id": "derek_analyst",
            "user_claims": ["basic_analyst"],
        }
    }

    list(execute_graph(graph, {"input_key": routing_token}, analyst_config))
    assert graph.get_state(analyst_config).next == ("interrupt_node",)

    unauthorized_config = {
        "configurable": {
            "thread_id": thread_id,
            "user_id": "guest_actor",
            "user_claims": ["unverified_guest"],
        }
    }

    with pytest.raises(PermissionError):
        list(resume(graph, routing_token, "Attempted Bypass", unauthorized_config))

    manager_config = {
        "configurable": {
            "thread_id": thread_id,
            "user_id": "baker_manager",
            "user_claims": ["executive_underwriter"],
        }
    }

    list(resume(graph, routing_token, "Manager Override Pass", manager_config))
    assert graph.get_state(manager_config).next == ()
