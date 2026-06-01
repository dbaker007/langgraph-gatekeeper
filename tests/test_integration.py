import uuid
from datetime import datetime, timedelta, timezone

import pytest

from core import execute_graph
from services.underwriting.workflow import ProjectState, graph
from ttl_monitor.monitor import run_ttl_monitor_cycle
from ttl_monitor.services import (
    clear_infrastructure_registry_tables,
    register_app,
    register_graph_asset,
)


@pytest.fixture(autouse=True)
def setup_test_db():
    clear_infrastructure_registry_tables()
    yield


def test_out_of_band_monitor_sla_breach():
    thread_id = f"integration_thread_{uuid.uuid4()}"
    thread_config = {
        "configurable": {
            "thread_id": thread_id,
            "user_id": "analyst_derek_123",
            "user_roles": ["basic_analyst"],
        }
    }

    register_graph_asset(
        graph_key="integration_credit_app",
        module_path="services.underwriting.workflow:graph",
    )

    past_timestamp = (datetime.now(timezone.utc) - timedelta(seconds=5)).strftime(
        "%Y-%m-%d %H:%M:%S"
    )
    register_app(
        thread_id=thread_id,
        graph_key="integration_credit_app",
        expiration_timestamp=past_timestamp,
    )

    state = ProjectState(
        application_id=thread_id,
        customer_name="Derek",
        customer_state="OH",
        household_income=15000000,
    )
    list(execute_graph(graph, state, thread_config))

    assert graph.get_state(thread_config).next == ("assign_agent",)
    assert run_ttl_monitor_cycle() == 1
    assert len(graph.get_state(thread_config).next) == 0
