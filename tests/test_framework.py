import os
import sqlite3
import uuid

import pytest
from langgraph.checkpoint.sqlite import SqliteSaver
from langgraph.graph import END, START, StateGraph
from pydantic import BaseModel

from langgraph_gatekeeper import (
    SecureWorkflowGateway,  # CHANGED: Imported the object gateway cleanly
    execute_graph,
    get_historical_thread_status,
    interrupt,
    resume,
)

MOCK_CHECKPOINT_DB = "test_checkpoints.db"


class FrameworkTestState(BaseModel):
    input_key: str = ""
    result_data: str = ""


def entry_node(state: FrameworkTestState) -> dict:
    return {}


def interrupt_node(state: FrameworkTestState) -> dict:
    response = interrupt(
        state.input_key,
        "framework_base_compliance",
        "verify_underwriter",
        {"status": "AWAITING_TEST_SIGN_OFF"},
    )
    return {"result_data": response}


# 1. INITIALIZE THE LIFECYCLE GATEWAY OBJECT
gateway = SecureWorkflowGateway()

# 2. CONFIG CHANNELS FLUENTLY UPFRONT
(
    gateway.enforce_entry("entry_node", required_claim="assign_analyst").enforce_entry(
        "interrupt_node", required_claim="assign_analyst"
    )
)

# 3. CONSTRUCT THE CANVAS TOPOLOGY
workflow = StateGraph(FrameworkTestState)
workflow.add_node("entry_node", entry_node)
workflow.add_node("interrupt_node", interrupt_node)

workflow.add_edge(START, "entry_node")
workflow.add_edge("entry_node", "interrupt_node")
workflow.add_edge("interrupt_node", END)

conn = sqlite3.connect(MOCK_CHECKPOINT_DB, check_same_thread=False)
checkpointer = SqliteSaver(conn)

# 4. BAKE THE SECURE GRAPH COMPILATION ASSET
graph = gateway.compile(workflow, checkpointer=checkpointer)


def test_full_orchestration_and_negative_failure_modes():
    thread_id = f"t_frame_{uuid.uuid4()}"
    config = {
        "configurable": {
            "thread_id": thread_id,
            "user_id": "derek",
            "user_claims": ["assign_analyst"],
        }
    }
    list(execute_graph(graph, {"input_key": f"TX_{uuid.uuid4()}"}, config))
    assert graph.get_state(config).next == ("interrupt_node",)


def test_resumption_retries_after_unauthorized_interception():
    thread_id = f"t_retry_{uuid.uuid4()}"
    routing_token = f"TOKEN_{uuid.uuid4()}"

    analyst_config = {
        "configurable": {
            "thread_id": thread_id,
            "user_id": "derek_analyst",
            "user_claims": ["assign_analyst"],
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
        list(
            resume(
                graph,
                "framework_base_compliance",
                "Attempted Bypass",
                unauthorized_config,
            )
        )

    manager_config = {
        "configurable": {
            "thread_id": thread_id,
            "user_id": "baker_manager",
            "user_claims": ["verify_underwriter"],
        }
    }

    list(
        resume(
            graph, "framework_base_compliance", "Manager Override Pass", manager_config
        )
    )
    assert graph.get_state(manager_config).next == ()


def test_immutable_history_retrieval_after_graph_reaches_end():
    thread_id = f"t_history_{uuid.uuid4()}"
    routing_token = f"TOKEN_HIST_{uuid.uuid4()}"

    analyst_config = {
        "configurable": {
            "thread_id": thread_id,
            "user_id": "derek_analyst",
            "user_claims": ["assign_analyst"],
        }
    }

    list(execute_graph(graph, {"input_key": routing_token}, analyst_config))

    pending_metrics = get_historical_thread_status(graph, analyst_config)
    assert pending_metrics["status"] == "PENDING"
    assert pending_metrics["thread_id"] == thread_id

    manager_config = {
        "configurable": {
            "thread_id": thread_id,
            "user_id": "baker_manager",
            "user_claims": ["verify_underwriter"],
        }
    }

    list(
        resume(
            graph,
            "framework_base_compliance",
            "Approved Final Ledger Settlement",
            manager_config,
        )
    )

    final_metrics = get_historical_thread_status(graph, manager_config)

    assert final_metrics["status"] == "PROCESSED"
    assert final_metrics["thread_id"] == thread_id
    assert "Approved Final Ledger Settlement" in final_metrics["resolution"]
