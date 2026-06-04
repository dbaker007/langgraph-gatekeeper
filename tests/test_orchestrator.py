import sqlite3
from typing import Any, Dict

import pytest
from langgraph.checkpoint.memory import MemorySaver
from langgraph.graph import END, START, StateGraph

from langgraph_gatekeeper.core.orchestrator import (
    execute_graph,
    interrupt,
    resume_by_context,
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
        # Enforce the strict framework interrupt contract requiring a business context token
        response = interrupt(
            routing_key="TEST_HAZMAT_KEY_999",
            business_context="hazmat_dispatch_compliance",
            payload={"status": "AWAITING_REVIEW"},
        )
        # Capture the human's response text and store it cleanly in the state logs
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
    """END-TO-END ORCHESTRATION VERIFICATION SUITE.

    Proves that the framework can securely freeze an active workflow graph, log its
    business context parameter, and subsequently unblock it using only what the
    resumer knows at the moment of impact: the thread_id and the business context constant.
    """
    thread_id = "thread_lifecycle_test_555"
    config = {
        "configurable": {
            "thread_id": thread_id,
            "user_id": "director_baker",
            "user_claims": ["cargo:read", "cargo:write"],
        }
    }

    # -------------------------------------------------------------------------
    # TURN 1: Initial pass. Run the engine stream forward into the interrupt hurdle
    # -------------------------------------------------------------------------
    events_turn_1 = list(execute_graph(compiled_test_graph, {}, config))

    # Verify the thread reached an isolated freeze point on disk
    snapshot_turn_1 = compiled_test_graph.get_state(config)
    assert snapshot_turn_1.next == ("gate_node",)

    # -------------------------------------------------------------------------
    # TURN 2: Resumption pass. Unblock utilizing our new composite key endpoint!
    # -------------------------------------------------------------------------
    manager_input_payload = "Approved and signed off under compliance profile Alpha."

    # We call the new single-responsibility function passing only what the resumer knows!
    events_turn_2 = list(
        resume_by_context(
            graph=compiled_test_graph,
            business_context="hazmat_dispatch_compliance",
            user_input=manager_input_payload,
            config=config,
        )
    )

    # Verify that the graph safely cleared the freeze state hurdle and ran straight to END
    final_snapshot = compiled_test_graph.get_state(config)
    assert not final_snapshot.next

    # Verify the state data channel captured the supervisor's text signature cleanly
    assert final_snapshot.values.get("manager_notes") == manager_input_payload


def test_resume_by_context_raises_value_error_if_composite_key_fails_to_match(
    compiled_test_graph,
):
    """SAFEGUARD: Verifies that resume_by_context crashes safely if an unrecognized context is passed."""
    thread_id = "thread_failure_test_666"
    config = {"configurable": {"thread_id": thread_id}}

    # Fire Turn 1 to seed an active interrupt under the hazmat context identifier
    list(execute_graph(compiled_test_graph, {}, config))

    # Attempting to resume using an unrecognized ghost context parameter string must drop a ValueError
    with pytest.raises(ValueError) as exc_info:
        list(
            resume_by_context(
                graph=compiled_test_graph,
                business_context="unrecognized_ghost_context",
                user_input="Should Fail",
                config=config,
            )
        )

    assert "No active interrupt token located for composite primary key pair" in str(
        exc_info.value
    )
