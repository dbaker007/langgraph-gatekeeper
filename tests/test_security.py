import sqlite3
import uuid
from typing import Optional

import pytest
from langgraph.checkpoint.base import RunnableConfig
from langgraph.checkpoint.sqlite import SqliteSaver
from langgraph.graph import END, START, StateGraph
from pydantic import BaseModel

# FIXED: Import the framework's custom wrapper interrupt instead of the native one!
from langgraph_gatekeeper import (
    compile_graph_with_authorization,
    execute_graph,
    interrupt,
)
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
    # FIXED: Pass all three strict positional parameters required by the framework contract!
    response = interrupt(
        routing_key=unique_key,
        business_context="hazmat_dispatch_compliance",
        payload={"status": "AWAITING_TEST_SIGN_OFF"},
    )
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
        db.execute("DELETE FROM tasks")
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


def test_complete_end_to_end_privilege_isolation_lifecycle():
    """LIFECYCLE SECURITY SUITE: Enforces strict multi-actor privilege separation.

    1. Analyst Derek initiates the request using 'basic_analyst' claims (Should Pass).
    2. Analyst Derek attempts to self-approve the thread on Turn 2 (Should Fail).
    3. Underwriter Baker attempts to approve the thread on Turn 2 holding only
       'executive_underwriter' claims (Should Pass).
    """
    from langgraph_gatekeeper import resume_by_context

    # REUSED Predictable Identifiers
    thread_id = f"t_lifecycle_{uuid.uuid4()}"
    # Use the specific context name string we track inside our logistics applications
    biz_ctx = "hazmat_dispatch_compliance"

    # -------------------------------------------------------------------------
    # STEP 1: Analyst Derek initiates the request (Should Pass)
    # -------------------------------------------------------------------------
    operator_config = {
        "configurable": {
            "thread_id": thread_id,
            "user_id": "operator_derek",
            "user_claims": ["basic_analyst"],
        }
    }
    list(execute_graph(mock_secure_graph, {}, operator_config))

    # Verify the thread successfully paused at the interrupt gate
    assert mock_secure_graph.get_state(operator_config).next == ("assign_agent",)

    # -------------------------------------------------------------------------
    # STEP 2: Analyst Derek attempts to self-approve the thread (Should Fail)
    # -------------------------------------------------------------------------
    with pytest.raises(PermissionError) as exc_info_operator:
        list(
            resume_by_context(
                graph=mock_secure_graph,
                business_context=biz_ctx,
                user_input="Operator Self-Approval Attempt",
                config=operator_config,
            )
        )
    assert "denied access" in str(exc_info_operator.value).lower()

    # -------------------------------------------------------------------------
    # STEP 3: Underwriter Baker attempts to approve the thread (Should Pass)
    # -------------------------------------------------------------------------
    director_config = {
        "configurable": {
            "thread_id": thread_id,
            "user_id": "underwriter_baker",
            "user_claims": [
                "executive_underwriter"
            ],  # ◄── Holds 'approve' but lacks 'execute'!
        }
    }

    list(
        resume_by_context(
            graph=mock_secure_graph,
            business_context=biz_ctx,
            user_input="Executive Underwriter Verification Sign-off",
            config=director_config,
        )
    )

    # Verify that the graph safely cleared the hurdle and ran all the way to completion
    final_snapshot = mock_secure_graph.get_state(director_config)
    assert not final_snapshot.next
