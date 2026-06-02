import uuid
from datetime import datetime, timedelta, timezone

import pytest

from langgraph_gatekeeper import execute_graph
from langgraph_gatekeeper.ttl_monitor.monitor import (
    run_ttl_monitor_cycle,
    set_framework_daemon_identity,
)
from langgraph_gatekeeper.ttl_monitor.services import (
    clear_infrastructure_registry_tables,
    register_app,
    register_graph_asset,
)
from tests.test_security import mock_secure_graph


@pytest.fixture(autouse=True)
def setup_test_db():
    clear_infrastructure_registry_tables()
    yield


def test_out_of_band_monitor_sla_breach_forces_eviction():
    """Validates that the daemon sweeps and evicts expired loops out-of-band."""
    thread_id = f"integration_thread_{uuid.uuid4()}"

    # 1. INITIAL RUN: Establishes the real historical database checkpoint holding at assign_agent
    initial_config = {
        "configurable": {
            "thread_id": thread_id,
            "user_id": "derek_analyst",
            "user_claims": ["basic_analyst"],
        }
    }
    list(execute_graph(mock_secure_graph, {}, initial_config))
    assert mock_secure_graph.get_state(initial_config).next == ("assign_agent",)

    # 2. SLA MONITOR REGISTRATION
    register_graph_asset(
        graph_key="test_credit_app",
        module_path="tests.test_security:mock_secure_graph",
    )
    past_timestamp = (datetime.now(timezone.utc) - timedelta(seconds=5)).strftime(
        "%Y-%m-%d %H:%M:%S"
    )
    register_app(
        thread_id=thread_id,
        graph_key="test_credit_app",
        expiration_timestamp=past_timestamp,
    )

    # 3. SERVICE M2M IDENTITY REGISTRATION
    LOGISTICS_DAEMON_IDENTITY = {
        "user_id": "logistics_automated_cron_worker",
        "user_claims": ["infra_eviction_clearance"],
    }
    set_framework_daemon_identity(LOGISTICS_DAEMON_IDENTITY)

    # 4. DAEMON CYCLE SWEEP: Reads the valid history on disk, and the stream executes as a clean no-op!
    assert run_ttl_monitor_cycle() == 1
