"""Domain Verification Suite: Out-of-Band SLA Daemon Evictions.

This suite exercises the background Time-To-Live (TTL) sweep monitors, verifying
that expired human-in-the-loop workflow thread instances are programmatically evicted
out-of-band by automated cron daemons (ttl_monitor/monitor.py).
"""

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


@pytest.fixture(autouse=True)
def setup_test_registry_tables():
    """Wipes infrastructure log maps clean between individual daemon test sweeps."""
    clear_infrastructure_registry_tables()
    yield


def test_out_of_band_monitor_sla_breach_forces_eviction(secure_ttl_graph_fixture):
    """SCENARIO: Verifies that when an out-of-band daemon cycle detects an expired
    thread checkpoint, it forcefully clears boundaries and triggers eviction nodes.
    """
    thread_id = f"integration_thread_{uuid.uuid4()}"
    initial_config = {
        "configurable": {
            "thread_id": thread_id,
            "user_id": "derek_analyst",
            "user_claims": ["assign_analyst"],
        }
    }

    # 1. Initialize the thread state cleanly inside our active canvas instance
    list(execute_graph(secure_ttl_graph_fixture, {"routing_key": ""}, initial_config))
    assert secure_ttl_graph_fixture.get_state(initial_config).next == (
        "assign_agent_ttl",
    )

    # 2. Register the microservice path reference mapping inside the SLA tables using the static asset string
    register_graph_asset(
        graph_key="test_credit_app",
        module_path="tests.conftest:secure_ttl_graph",
    )

    # 3. Simulate an out-of-band SLA deadline breach by logging a past expiration timestamp
    past_timestamp = (datetime.now(timezone.utc) - timedelta(seconds=5)).strftime(
        "%Y-%m-%d %H:%M:%S"
    )
    register_app(
        thread_id=thread_id,
        graph_key="test_credit_app",
        expiration_timestamp=past_timestamp,
    )

    # 4. Authenticate the automated cron worker machine-to-machine identity context
    set_framework_daemon_identity(
        {
            "user_id": "logistics_automated_cron_worker",
            "user_claims": ["infra_eviction_clearance", "mutate_state"],
        }
    )

    # 5. Fire the background daemon collector sweep and assert exactly 1 eviction clears
    assert run_ttl_monitor_cycle() == 1
