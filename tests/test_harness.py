import sqlite3
import uuid

import pytest

from core import execute_graph, resume
from services.underwriting.workflow import ProjectState, graph


@pytest.fixture
def thread_config():
    return {
        "configurable": {
            "thread_id": f"test_thread_{uuid.uuid4()}",
            "user_id": "analyst_derek_123",
            "user_roles": ["basic_analyst"],
        }
    }


def test_vip_parallel_path_approved(thread_config):
    state = ProjectState(
        application_id=thread_config["configurable"]["thread_id"],
        customer_name="Derek",
        customer_state="OH",
        household_income=15000000,
    )

    list(execute_graph(graph, state, thread_config))
    snapshot_1 = graph.get_state(thread_config)
    assert snapshot_1.next == ("assign_agent",)

    # Corrected tuple indexing for modern LangGraph versions
    stage_1_routing_key = snapshot_1.tasks[0].interrupts[0].value["routing_key"]
    list(resume(graph, stage_1_routing_key, "Nathan", thread_config))

    snapshot_2 = graph.get_state(thread_config)
    assert snapshot_2.next == ("assign_credit",)

    # Corrected tuple indexing for modern LangGraph versions
    stage_2_routing_key = snapshot_2.tasks[0].interrupts[0].value["routing_key"]

    thread_config["configurable"]["user_id"] = "executive_architect_789"
    thread_config["configurable"]["user_roles"] = ["executive_underwriter"]

    list(resume(graph, stage_2_routing_key, "VIP_APPROVED", thread_config))
    final_snapshot = graph.get_state(thread_config)
    assert final_snapshot.next == ()
