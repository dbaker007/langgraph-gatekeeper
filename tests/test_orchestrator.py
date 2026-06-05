import sqlite3
import uuid
from typing import Any, Dict

import pytest
from langgraph.checkpoint.memory import MemorySaver
from langgraph.graph import END, START, StateGraph

from langgraph_gatekeeper.core.orchestrator import (
    execute_graph,
    interrupt,
    resume,
)

# Import the core orchestrator and ledger components natively
from langgraph_gatekeeper.core.task_cache_db import (
    TASK_CACHE_DB_PATH,
    init_task_cache_db,
)


@pytest.fixture(autouse=True)
def setup_test_db():
    """Ensures a clean database tracking registry state before every single test pass."""
    init_task_cache_db()
    with sqlite3.connect(TASK_CACHE_DB_PATH) as conn:
        conn.execute("DELETE FROM tasks")
        conn.commit()


@pytest.fixture
def compiled_test_graph():
    """Compiles an authentic, live StateGraph canvas that implements human-in-the-loop branching."""

    def test_gate_node(state: Dict[str, Any]) -> dict:
        response = interrupt(
            "TEST_HAZMAT_KEY_999",
            "hazmat_dispatch_compliance",
            "executive_underwriter",
            {"status": "AWAITING_REVIEW"},
        )
        return {"manager_notes": str(response)}

    workflow = StateGraph(dict)
    workflow.add_node("gate_node", test_gate_node)
    workflow.add_edge(START, "gate_node")
    workflow.add_edge("gate_node", END)

    return workflow.compile(checkpointer=MemorySaver())


# =============================================================================
# ORCHESTRATION LAYER INTEGRATION TESTS
# =============================================================================


def test_full_orchestration_lifecycle_using_composite_context_resumption(
    compiled_test_graph,
):
    """END-TO-END ORCHESTRATION VERIFICATION SUITE."""
    thread_id = "thread_lifecycle_test_555"
    config = {
        "configurable": {
            "thread_id": thread_id,
            "user_id": "director_baker",
            "user_claims": ["cargo:read", "cargo:write"],
        }
    }

    list(execute_graph(compiled_test_graph, {}, config))

    snapshot_turn_1 = compiled_test_graph.get_state(config)
    assert snapshot_turn_1.next == ("gate_node",)

    manager_input_payload = "Approved and signed off under compliance profile Alpha."

    events_turn_2 = list(
        resume(
            graph=compiled_test_graph,
            business_context="hazmat_dispatch_compliance",
            user_input=manager_input_payload,
            config=config,
        )
    )

    final_snapshot = compiled_test_graph.get_state(config)
    assert not final_snapshot.next
    assert final_snapshot.values.get("manager_notes") == manager_input_payload


def test_resume_by_context_raises_value_error_if_composite_key_fails_to_match(
    compiled_test_graph,
):
    """SAFEGUARD: Verifies that resume_by_context crashes safely if an unrecognized context is passed."""
    thread_id = "thread_failure_test_666"
    config = {"configurable": {"thread_id": thread_id}}

    list(execute_graph(compiled_test_graph, {}, config))

    with pytest.raises(ValueError) as exc_info:
        list(
            resume(
                graph=compiled_test_graph,
                business_context="unrecognized_ghost_context",
                user_input="Should Fail",
                config=config,
            )
        )

    assert "No active interrupt token located for composite primary key pair" in str(
        exc_info.value
    )


def test_resumption_tuple_string_regression_protection(compiled_test_graph):
    """REGRESSION SUITE: Verifies that raw string extraction from row data objects is clean."""
    thread_id = "thread_tuple_protection_777"
    config = {
        "configurable": {
            "thread_id": thread_id,
            "user_id": "director_baker",
            "user_claims": ["cargo:read", "cargo:write"],
        }
    }

    list(execute_graph(compiled_test_graph, {}, config))

    complex_input_signature = (
        "Approved, signed, and certified (Compliance Vector: 'Alpha-7', Node: #4)."
    )

    list(
        resume(
            graph=compiled_test_graph,
            business_context="hazmat_dispatch_compliance",
            user_input=complex_input_signature,
            config=config,
        )
    )

    final_snapshot = compiled_test_graph.get_state(config)
    assert not final_snapshot.next
    assert final_snapshot.values.get("manager_notes") == complex_input_signature


@pytest.fixture
def consecutive_collision_graph():
    """Compiles a StateGraph with two consecutive human-in-the-loop nodes."""

    def node_alpha(state: Dict[str, Any]) -> dict:
        response = interrupt(
            "KEY_ALPHA_111",
            "shared_business_context",
            "executive_underwriter",
            {"step": "ALPHA"},
        )
        return {"notes_alpha": str(response)}

    def node_beta(state: Dict[str, Any]) -> dict:
        response = interrupt(
            "KEY_BETA_222",
            "shared_business_context",
            "executive_underwriter",
            {"step": "BETA"},
        )
        return {"notes_beta": str(response)}

    workflow = StateGraph(dict)
    workflow.add_node("node_alpha", node_alpha)
    workflow.add_node("node_beta", node_beta)

    workflow.add_edge(START, "node_alpha")
    workflow.add_edge("node_alpha", "node_beta")
    workflow.add_edge("node_beta", END)

    return workflow.compile(checkpointer=MemorySaver())


def test_orchestrator_bubbles_up_database_collision_exceptions(compiled_test_graph):
    """API INTEGRITY SUITE: Verifies database exceptions bubble out of the core orchestrator."""

    def dev_two_node(state: Dict[str, Any]) -> dict:
        interrupt(
            "DEV_TWO_UNIQUE_ROUTING_KEY",
            "hazmat_dispatch_compliance",
            "executive_underwriter",
            {"status": "DEV_TWO_ATTEMPT"},
        )
        return {}

    builder = StateGraph(dict)
    builder.add_node("dev_two_gate", dev_two_node)
    builder.add_edge(START, "dev_two_gate")
    builder.add_edge("dev_two_gate", END)
    dev_two_compiled_graph = builder.compile(checkpointer=MemorySaver())

    thread_id = "thread_multi_developer_collision_999"
    config = {
        "configurable": {
            "thread_id": thread_id,
            "user_id": "director_baker",
            "user_claims": ["admin"],
        }
    }

    list(execute_graph(compiled_test_graph, {}, config))

    with pytest.raises(sqlite3.IntegrityError):
        list(execute_graph(dev_two_compiled_graph, {}, config))


def test_orchestrator_saves_and_surfaces_required_claim_metadata_lifecycle():
    """TDD FOCUS - TASK 1: Verifies the public orchestrator tracks 'required_claim'."""
    from langgraph_gatekeeper.core.task_cache_db import get_token_by_business_context

    def node_strict_claim(state: dict) -> dict:
        interrupt("STRICT_TDD_KEY_111", "strict_context", "executive_underwriter", {})
        return {}

    builder_1 = StateGraph(dict)
    builder_1.add_node("gate_strict", node_strict_claim)
    builder_1.add_edge(START, "gate_strict")
    builder_1.add_edge("gate_strict", END)
    graph_strict = builder_1.compile(checkpointer=MemorySaver())

    config_strict = {"configurable": {"thread_id": "thread_strict_tdd_111"}}
    list(execute_graph(graph_strict, {}, config_strict))

    token_data_strict = get_token_by_business_context(
        "thread_strict_tdd_111", "strict_context"
    )
    assert token_data_strict is not None
    assert token_data_strict["required_claim"] == "executive_underwriter"

    def node_no_permission(state: dict) -> dict:
        interrupt("OPEN_TDD_KEY_222", "open_context", "", {})
        return {}

    builder_2 = StateGraph(dict)
    builder_2.add_node("gate_open", node_no_permission)
    builder_2.add_edge(START, "gate_open")
    builder_2.add_edge("gate_open", END)
    graph_open = builder_2.compile(checkpointer=MemorySaver())

    config_open = {"configurable": {"thread_id": "thread_open_tdd_222"}}
    list(execute_graph(graph_open, {}, config_open))

    token_data_open = get_token_by_business_context(
        "thread_open_tdd_222", "open_context"
    )
    assert token_data_open is not None
    assert token_data_open["required_claim"] == ""
