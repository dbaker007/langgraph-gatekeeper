import sqlite3
import uuid
from typing import Any, Optional

import pytest
from langgraph.checkpoint.base import RunnableConfig
from langgraph.checkpoint.sqlite import SqliteSaver
from langgraph.graph import END, START, StateGraph
from pydantic import BaseModel

from core import compile_graph_with_authorization, execute_graph, interrupt, resume
from core.task_cache_db import TASK_CACHE_DB_PATH

MOCK_CHECKPOINT_DB = "test_checkpoints.db"


class MockState(BaseModel):
    routing_key: str = ""
    result_data: str = ""


def assign_agent_node(
    state: MockState, config: Optional[RunnableConfig] = None
) -> dict:
    unique_key = f"task_concierge_{uuid.uuid4()}"
    response = interrupt({"routing_key": unique_key})
    return {"routing_key": unique_key, "result_data": response}


workflow = StateGraph(MockState)
workflow.add_node("assign_agent", assign_agent_node)
workflow.add_edge(START, "assign_agent")
workflow.add_edge("assign_agent", END)

conn = sqlite3.connect(MOCK_CHECKPOINT_DB, check_same_thread=False)
checkpointer = SqliteSaver(conn)
mock_secure_graph = compile_graph_with_authorization(
    workflow, checkpointer=checkpointer
)


@pytest.fixture(autouse=True)
def cleanup_test_environments():
    yield
    with sqlite3.connect(TASK_CACHE_DB_PATH) as db:
        db.execute("DELETE FROM active_tasks")
        db.commit()


def test_security_lifecycle_passed():
    thread_config = {
        "configurable": {
            "thread_id": f"test_thread_{uuid.uuid4()}",
            "user_id": "analyst_derek_123",
            "user_roles": ["basic_analyst"],
        }
    }
    list(execute_graph(mock_secure_graph, {}, thread_config))
    snapshot_1 = mock_secure_graph.get_state(thread_config)
    assert snapshot_1.next == ("assign_agent",)

    # Corrected tuple indexing for modern LangGraph versions
    active_task_1 = snapshot_1.tasks[0]
    stage_1_routing_key = active_task_1.interrupts[0].value["routing_key"]

    list(resume(mock_secure_graph, stage_1_routing_key, "Nathan", thread_config))
    final_snapshot = mock_secure_graph.get_state(thread_config)
    assert len(final_snapshot.next) == 0


def test_security_lifecycle_failed_on_unauthorized_resume():
    thread_config = {
        "configurable": {
            "thread_id": f"test_thread_{uuid.uuid4()}",
            "user_id": "analyst_derek_123",
            "user_roles": ["basic_analyst"],
        }
    }
    list(execute_graph(mock_secure_graph, {}, thread_config))
    snapshot_1 = mock_secure_graph.get_state(thread_config)
    assert snapshot_1.next == ("assign_agent",)

    # Corrected tuple indexing for modern LangGraph versions
    active_task_1 = snapshot_1.tasks[0]
    stage_1_routing_key = active_task_1.interrupts[0].value["routing_key"]

    thread_config["configurable"]["user_id"] = "malicious_actor_999"
    thread_config["configurable"]["user_roles"] = ["unverified_guest"]

    with pytest.raises(PermissionError) as exc_info:
        list(
            resume(mock_secure_graph, stage_1_routing_key, "FORGED_DATA", thread_config)
        )
    assert "SECURITY INTERCEPTION" in str(exc_info.value)


def test_resumption_fallback_nuance():
    thread_config = {
        "configurable": {
            "thread_id": f"test_thread_{uuid.uuid4()}",
            "user_id": "analyst_derek_123",
            "user_roles": ["basic_analyst"],
        }
    }
    list(execute_graph(mock_secure_graph, {}, thread_config))
    snapshot = mock_secure_graph.get_state(thread_config)

    # Corrected tuple indexing for modern LangGraph versions
    fb_key = snapshot.tasks[0].interrupts[0].value["routing_key"]

    list(resume(mock_secure_graph, fb_key, "FALLBACK_SUCCESS", thread_config))
    final_snapshot = mock_secure_graph.get_state(thread_config)
    assert len(final_snapshot.next) == 0
