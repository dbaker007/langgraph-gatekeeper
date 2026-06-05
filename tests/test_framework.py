import os
import sqlite3
import uuid

import pytest
from langgraph.checkpoint.sqlite import SqliteSaver
from langgraph.graph import END, START, StateGraph
from pydantic import BaseModel

from langgraph_gatekeeper import (
    compile_graph_with_authorization,
    execute_graph,
    get_historical_thread_status,
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
    # FIXED: Added the mandatory required_claim positional token matching our TDD task 1 layout!
    response = interrupt(
        state.input_key,
        "framework_base_compliance",
        "executive_underwriter",
        {"status": "AWAITING_TEST_SIGN_OFF"},
    )
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
    list(execute_graph(graph, {"input_key": f"TX_{uuid.uuid4()}"}, config))
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


def test_immutable_history_retrieval_after_graph_reaches_end():
    """Validates that even after a workflow has navigated to END, its thread_id pointer
    is retained, and its historical resolution ledger remains fully queryable out-of-band.
    """
    thread_id = f"t_history_{uuid.uuid4()}"
    routing_token = f"TOKEN_HIST_{uuid.uuid4()}"

    analyst_config = {
        "configurable": {
            "thread_id": thread_id,
            "user_id": "derek_analyst",
            "user_claims": ["basic_analyst"],
        }
    }

    # 1. INITIAL PASS: Launch the thread and halt at the interrupt node
    list(execute_graph(graph, {"input_key": routing_token}, analyst_config))

    # FIXED: Update historical tracking test assertion configuration parameters to match the unified config contract!
    pending_metrics = get_historical_thread_status(graph, analyst_config)
    assert pending_metrics["status"] == "PENDING"
    assert pending_metrics["thread_id"] == thread_id

    # 2. RESUMPTION PASS: Clear the gate using an authorized manager token to push the graph to END
    manager_config = {
        "configurable": {
            "thread_id": thread_id,
            "user_id": "baker_manager",
            "user_claims": ["executive_underwriter"],
        }
    }

    list(
        resume(graph, routing_token, "Approved Final Ledger Settlement", manager_config)
    )

    # 3. HISTORY LOOKUP PASS: Query the status long after the graph is dead and gone!
    final_metrics = get_historical_thread_status(graph, manager_config)

    assert final_metrics["status"] == "PROCESSED"
    assert final_metrics["thread_id"] == thread_id
    assert "Approved Final Ledger Settlement" in final_metrics["resolution"]
