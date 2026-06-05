import sqlite3
import uuid
from typing import Optional

import pytest
from langgraph.checkpoint.base import RunnableConfig
from langgraph.checkpoint.memory import MemorySaver
from langgraph.checkpoint.sqlite import SqliteSaver
from langgraph.graph import END, START, StateGraph
from pydantic import BaseModel

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
    # Aligned positional format using action-based permission verbs
    response = interrupt(
        unique_key,
        "hazmat_dispatch_compliance",
        "verify_underwriter",  # CHANGED: Action permission for hurdle
        {"status": "AWAITING_TEST_SIGN_OFF"},
    )
    return {"routing_key": unique_key, "result_data": response}


def kill_switch(state: MockState, config: Optional[RunnableConfig] = None) -> dict:
    return {}


workflow = StateGraph(MockState)
workflow.add_node("assign_agent", assign_agent_node)
workflow.add_node("kill_switch", kill_switch)

workflow.add_edge(START, "assign_agent")
workflow.add_edge("assign_agent", END)
workflow.add_edge("kill_switch", END)

conn = sqlite3.connect(MOCK_CHECKPOINT_DB, check_same_thread=False)
checkpointer = SqliteSaver(conn)

# Fluent compiler syntax with fine-grained permission naming
mock_secure_graph = (
    compile_graph_with_authorization(workflow, checkpointer=checkpointer)
    .enforce_entry(
        "assign_agent", required_claim="assign_analyst"
    )  # CHANGED: Action permission
    .enforce_entry("kill_switch", required_claim="infra_eviction_clearance")
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
            "user_claims": ["assign_analyst"],  # CHANGED: Matches entry gate permission
        }
    }
    list(execute_graph(mock_secure_graph, {}, config))
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
    """LIFECYCLE SECURITY SUITE: Enforces strict multi-actor privilege separation."""
    from langgraph_gatekeeper import resume  # CHANGED: Swapped for renamed resume hook

    thread_id = f"t_lifecycle_{uuid.uuid4()}"
    biz_ctx = "hazmat_dispatch_compliance"

    operator_config = {
        "configurable": {
            "thread_id": thread_id,
            "user_id": "operator_derek",
            "user_claims": ["assign_analyst"],  # CHANGED: Action-based naming
        }
    }
    list(execute_graph(mock_secure_graph, {}, operator_config))

    assert mock_secure_graph.get_state(operator_config).next == ("assign_agent",)

    with pytest.raises(PermissionError) as exc_info_operator:
        list(
            resume(
                graph=mock_secure_graph,
                business_context=biz_ctx,
                user_input="Operator Self-Approval Attempt",
                config=operator_config,
            )
        )
    assert "denied" in str(exc_info_operator.value).lower()

    director_config = {
        "configurable": {
            "thread_id": thread_id,
            "user_id": "underwriter_baker",
            "user_claims": ["verify_underwriter"],  # Isolated manager claim
        }
    }

    list(
        resume(
            graph=mock_secure_graph,
            business_context=biz_ctx,
            user_input="Executive Underwriter Verification Sign-off",
            config=director_config,
        )
    )

    final_snapshot = mock_secure_graph.get_state(director_config)
    assert not final_snapshot.next


def test_fail_closed_firewall_blocks_spoof_attempts_with_missing_user_id():
    """SECURITY SUITE - TASK 2: Enforces fail-closed protection against anonymous hacks."""

    def secure_node(state: dict) -> dict:
        return {"result": "hack_succeeded"}

    local_workflow = StateGraph(dict)
    local_workflow.add_node("restricted_target", secure_node)
    local_workflow.add_edge(START, "restricted_target")
    local_workflow.add_edge("restricted_target", END)

    malicious_secure_graph = compile_graph_with_authorization(
        local_workflow, checkpointer=MemorySaver()
    ).enforce_entry("restricted_target", required_claim="admin_clearance")

    malicious_config = {"configurable": {"thread_id": f"t_attack_{uuid.uuid4()}"}}

    with pytest.raises(PermissionError) as exc_info:
        list(execute_graph(malicious_secure_graph, {}, malicious_config))

    assert "SECURITY INTERCEPTION" in str(exc_info.value)


def test_framework_permits_internal_checkpointer_serialization_passes():
    """SECURITY SUITE - TASK 2: Verifies LangGraph's cleanup crew can save state."""

    def functional_node(state: dict) -> dict:
        return {"status": "node_processed"}

    local_workflow = StateGraph(dict)
    local_workflow.add_node("functional_gate", functional_node)
    local_workflow.add_edge(START, "functional_gate")
    local_workflow.add_edge("functional_gate", END)

    functional_secure_graph = compile_graph_with_authorization(
        local_workflow, checkpointer=MemorySaver()
    ).enforce_entry("functional_gate", required_claim="standard_user")

    valid_user_config = {
        "configurable": {
            "thread_id": f"t_clean_pass_{uuid.uuid4()}",
            "user_id": "user_derek",
            "user_claims": ["standard_user"],
        }
    }

    list(execute_graph(functional_secure_graph, {}, valid_user_config))

    final_state = functional_secure_graph.get_state(valid_user_config)
    assert final_state.next == ()
