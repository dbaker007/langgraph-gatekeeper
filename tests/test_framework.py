import sqlite3
import uuid

import pytest
from langgraph.checkpoint.sqlite import SqliteSaver
from langgraph.graph import END, START, StateGraph
from langgraph.types import interrupt
from pydantic import BaseModel

from langgraph_gatekeeper import compile_graph_with_authorization, execute_graph, resume

MOCK_CHECKPOINT_DB = "test_checkpoints.db"


class MockState(BaseModel):
    routing_key: str = ""


def interrupt_node(state: MockState) -> dict:
    unique_key = f"task_token_{uuid.uuid4()}"
    response = interrupt({"routing_key": unique_key})
    return {"routing_key": unique_key}


workflow = StateGraph(MockState)
workflow.add_node("interrupt_node", interrupt_node)
workflow.add_edge(START, "interrupt_node")
workflow.add_edge("interrupt_node", END)

conn = sqlite3.connect(MOCK_CHECKPOINT_DB, check_same_thread=False)
checkpointer = SqliteSaver(conn)
graph = compile_graph_with_authorization(
    workflow, checkpointer=checkpointer, policy_provider={}
)


@pytest.fixture
def thread_config():
    return {"configurable": {"thread_id": f"test_thread_{uuid.uuid4()}"}}


def test_full_orchestration_and_negative_failure_modes(thread_config):
    # 1. FRESH INITIAL RUN
    list(execute_graph(graph, {}, thread_config))
    snapshot = graph.get_state(thread_config)
    assert snapshot.next == ("interrupt_node",)

    # Extract the token string from the native snapshot tasks tuple
    active_task_1 = snapshot.tasks[0]
    stage_1_routing_key = active_task_1.interrupts[0].value["routing_key"]

    # 2. NEGATIVE TEST 1: Resuming with a non-existent/forged key breaks with ValueError
    with pytest.raises(ValueError) as exc_info:
        list(
            resume(
                graph,
                routing_key="malicious_forged_key",
                user_input="data",
                config=thread_config,
            )
        )
    assert "No active interrupt token found" in str(exc_info.value)

    # 3. POSITIVE RESUME: Wakes up and completes cleanly
    list(resume(graph, stage_1_routing_key, "Valid Input", thread_config))
    assert graph.get_state(thread_config).next == ()

    # 4. NEGATIVE TEST 2: Replay attack (Double Resume) gets rejected with ValueError
    with pytest.raises(ValueError) as exc_info:
        list(resume(graph, stage_1_routing_key, "Replay Attack Data", thread_config))
    assert "No active interrupt token found" in str(exc_info.value)
