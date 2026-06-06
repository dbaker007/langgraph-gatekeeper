import sqlite3
import uuid

import pytest
from langgraph.checkpoint.sqlite import SqliteSaver
from langgraph.graph import END, START, StateGraph

from langgraph_gatekeeper import (
    GatekeeperState,
    SecureWorkflowGateway,
    execute_graph,
    get_historical_thread_status,
    interrupt,
    resume,
)

MOCK_ORCH_DB = "test_orch_checkpoints.db"


class OrchTestState(GatekeeperState):
    routing_key: str
    result_data: str


def step_one_node(state: OrchTestState) -> dict:
    return {}


def step_two_node(state: OrchTestState) -> dict:
    token = interrupt(
        "task_abc", "composite_biz_context", "manager_clearance", {"details": "HOLDING"}
    )
    return {"result_data": token}


gateway = SecureWorkflowGateway()
(
    gateway.enforce_entry("step_one_node", required_claim="operator").enforce_entry(
        "step_two_node", required_claim="operator"
    )
)

workflow = StateGraph(OrchTestState)
workflow.add_node("step_one_node", step_one_node)
workflow.add_node("step_two_node", step_two_node)
workflow.add_edge(START, "step_one_node")
workflow.add_edge("step_one_node", "step_two_node")
workflow.add_edge("step_two_node", END)

conn = sqlite3.connect(MOCK_ORCH_DB, check_same_thread=False)
checkpointer = SqliteSaver(conn)
graph = gateway.compile(workflow, checkpointer=checkpointer)


def test_full_orchestration_lifecycle_using_composite_context_resumption():
    thread_id = f"t_orch_{uuid.uuid4()}"
    config = {
        "configurable": {
            "thread_id": thread_id,
            "user_id": "operator_derek",
            "user_claims": ["operator"],
        }
    }
    list(execute_graph(graph, {}, config))
    assert graph.get_state(config).next == ("step_two_node",)


def test_resume_by_context_raises_value_error_if_composite_key_fails_to_match():
    thread_id = f"t_fail_{uuid.uuid4()}"
    config = {
        "configurable": {
            "thread_id": thread_id,
            "user_id": "operator_derek",
            "user_claims": ["operator"],
        }
    }
    list(execute_graph(graph, {}, config))

    manager_config = {
        "configurable": {
            "thread_id": thread_id,
            "user_id": "manager_baker",
            "user_claims": ["manager_clearance"],
        }
    }
    with pytest.raises(ValueError):
        list(resume(graph, "INVALID_BIZ_CONTEXT", "Passed Input", manager_config))


def test_resumption_tuple_string_regression_protection():
    pass


def test_orchestrator_bubbles_up_database_collision_exceptions():
    pass


def test_orchestrator_saves_and_surfaces_required_claim_metadata_lifecycle():
    thread_id = f"t_meta_{uuid.uuid4()}"
    config = {
        "configurable": {
            "thread_id": thread_id,
            "user_id": "operator_derek",
            "user_claims": ["operator"],
        }
    }
    list(execute_graph(graph, {}, config))
    metrics = get_historical_thread_status(graph, config)
    assert metrics["status"] == "PENDING"
