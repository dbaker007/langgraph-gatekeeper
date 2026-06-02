import sqlite3
import uuid
from typing import Optional

import pytest
from langgraph.checkpoint.base import RunnableConfig
from langgraph.checkpoint.sqlite import SqliteSaver
from langgraph.graph import END, START, StateGraph

# Native framework interrupt hook
from langgraph.types import interrupt
from pydantic import BaseModel

from langgraph_gatekeeper import compile_graph_with_authorization, execute_graph
from langgraph_gatekeeper.core.task_cache_db import TASK_CACHE_DB_PATH

MOCK_CHECKPOINT_DB = "test_checkpoints.db"


class MockState(BaseModel):
    routing_key: str = ""
    result_data: str = ""
    kill_switch_reason: str = ""


def assign_agent_node(
    state: MockState, config: Optional[RunnableConfig] = None
) -> dict:
    unique_key = f"task_concierge_{uuid.uuid4()}"
    # FIXED: Add the required interrupt hurdle to force the thread to freeze
    response = interrupt({"routing_key": unique_key})
    return {"routing_key": unique_key, "result_data": response}


def kill_switch(state: MockState, config: Optional[RunnableConfig] = None) -> dict:
    return {}


SIMPLE_POLICIES = {
    "assign_agent": {"execute": "basic_analyst", "approve": "executive_underwriter"},
    "kill_switch": {"execute": "infra_eviction_clearance"},
}

workflow = StateGraph(MockState)
workflow.add_node("assign_agent", assign_agent_node)
workflow.add_node("kill_switch", kill_switch)

workflow.add_edge(START, "assign_agent")
workflow.add_edge("assign_agent", END)
workflow.add_edge("kill_switch", END)

conn = sqlite3.connect(MOCK_CHECKPOINT_DB, check_same_thread=False)
checkpointer = SqliteSaver(conn)

mock_secure_graph = compile_graph_with_authorization(
    workflow, checkpointer=checkpointer, policy_provider=SIMPLE_POLICIES
)


@pytest.fixture(autouse=True)
def cleanup_test_environments():
    yield
    with sqlite3.connect(TASK_CACHE_DB_PATH) as db:
        db.execute("DELETE FROM active_tasks")
        db.commit()


def test_initial_execution_clears_gate_with_correct_role():
    config = {
        "configurable": {
            "thread_id": f"t_{uuid.uuid4()}",
            "user_id": "derek",
            "user_claims": ["basic_analyst"],
        }
    }
    list(execute_graph(mock_secure_graph, {}, config))
    # Asserts that the graph successfully pauses at the interrupt hurdle
    assert mock_secure_graph.get_state(config).next == ("assign_agent",)


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
