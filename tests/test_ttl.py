import sqlite3
import uuid
from datetime import datetime, timedelta, timezone
from typing import Optional

import pytest
from langgraph.checkpoint.base import RunnableConfig
from langgraph.checkpoint.sqlite import SqliteSaver
from langgraph.graph import END, START, StateGraph
from pydantic import BaseModel

from langgraph_gatekeeper import (
    compile_graph_with_authorization,
    execute_graph,
    interrupt,
)
from langgraph_gatekeeper.ttl_monitor.monitor import (
    run_ttl_monitor_cycle,
    set_framework_daemon_identity,
)
from langgraph_gatekeeper.ttl_monitor.services import (
    clear_infrastructure_registry_tables,
    register_app,
    register_graph_asset,
)

MOCK_TTL_CHECKPOINT_DB = "test_ttl_checkpoints.db"


class MockTtlState(BaseModel):
    routing_key: str = ""
    result_data: str = ""


def assign_agent_ttl_node(
    state: MockTtlState, config: Optional[RunnableConfig] = None
) -> dict:
    unique_key = f"task_ttl_concierge_{uuid.uuid4()}"
    # Aligned positional arguments for modern 4-parameter signature contract
    response = interrupt(
        unique_key,
        "hazmat_dispatch_compliance",
        "executive_underwriter",
        {"status": "AWAITING_TTL_SIGN_OFF"},
    )
    return {"routing_key": unique_key, "result_data": response}


# COMPLIANCE MANDATE: Abstract system eviction target node
def kill_switch(state: MockTtlState, config: Optional[RunnableConfig] = None) -> dict:
    return {}


workflow = StateGraph(MockTtlState)
workflow.add_node("assign_agent_ttl", assign_agent_ttl_node)
workflow.add_node("kill_switch", kill_switch)  # Registering the mandatory system node

workflow.add_edge(START, "assign_agent_ttl")
workflow.add_edge("assign_agent_ttl", END)
workflow.add_edge("kill_switch", END)

conn = sqlite3.connect(MOCK_TTL_CHECKPOINT_DB, check_same_thread=False)
checkpointer = SqliteSaver(conn)

# Fully isolated local graph asset setup cut from test_security.py
mock_secure_ttl_graph = (
    compile_graph_with_authorization(workflow, checkpointer=checkpointer)
    .enforce_entry("assign_agent_ttl", required_claim="basic_analyst")
    .enforce_entry("kill_switch", required_claim="infra_eviction_clearance")
)


@pytest.fixture(autouse=True)
def setup_test_db():
    clear_infrastructure_registry_tables()
    yield


def test_out_of_band_monitor_sla_breach_forces_eviction():
    """Validates that the daemon sweeps and evicts expired loops out-of-band."""
    thread_id = f"integration_thread_{uuid.uuid4()}"

    initial_config = {
        "configurable": {
            "thread_id": thread_id,
            "user_id": "derek_analyst",
            "user_claims": ["basic_analyst"],
        }
    }
    list(execute_graph(mock_secure_ttl_graph, {}, initial_config))
    assert mock_secure_ttl_graph.get_state(initial_config).next == ("assign_agent_ttl",)

    # 2. SLA MONITOR REGISTRATION
    register_graph_asset(
        graph_key="test_credit_app",
        module_path="tests.test_ttl:mock_secure_ttl_graph",
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
        "user_claims": ["infra_eviction_clearance", "mutate_state"],
    }
    set_framework_daemon_identity(LOGISTICS_DAEMON_IDENTITY)

    assert run_ttl_monitor_cycle() == 1
