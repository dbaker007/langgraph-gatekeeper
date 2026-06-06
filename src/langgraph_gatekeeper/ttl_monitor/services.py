import importlib
import sqlite3

from langgraph_gatekeeper.core.graph import SecureCompiledGraph

# CORRECTED: Pull from our new package namespace structure
from langgraph_gatekeeper.ttl_monitor.sla_db import (
    SLA_DB_PATH,
    init_sla_db,
    insert_sla_timer,
    mark_sla_thread_complete,
)


def clear_infrastructure_registry_tables() -> None:
    """Service Utility: Truncates and resets local tracking databases out-of-band.

    Guarantees a clean slate for integration tests without exposing file handles
    or raw SQL strings to the application layers.
    """
    init_sla_db()
    with sqlite3.connect(SLA_DB_PATH, timeout=30.0) as conn:
        conn.execute("DELETE FROM sla_timers")
        conn.execute("DELETE FROM graph_registry")
        conn.commit()


def register_graph_asset(graph_key: str, module_path: str) -> None:
    """Registers a compiled graph microservice reference path into the background daemon registry.

    Verifies that the targeted asset exists and conforms to framework type expectations before
    committing the record to the system topology map.
    """
    import importlib

    from langgraph_gatekeeper.core.graph import SecureCompiledGraph

    # LOCKED IN: Consume your pre-existing persistence primitive natively
    from langgraph_gatekeeper.ttl_monitor.sla_db import insert_graph_registry

    # 1. Dynamically parse and resolve the module string tracking parameters
    if ":" not in module_path:
        raise ValueError(
            f"Invalid module reference path format '{module_path}'. Expected format 'module.submodule:asset_name'."
        )

    mod_name, attr_name = module_path.split(":")

    try:
        mod = importlib.import_module(mod_name)
    except ImportError as imp_err:
        raise ImportError(
            f"Failed to dynamically resolve module path '{mod_name}'. Details: {str(imp_err)}"
        )

    if not hasattr(mod, attr_name):
        raise AttributeError(
            f"The module '{mod_name}' does not possess a compiled attribute asset named '{attr_name}'."
        )

    graph_obj = getattr(mod, attr_name)

    # 2. Accept BOTH raw LangGraph compiled structures and our framework's SecureCompiledGraph proxy
    is_valid_graph_primitive = False
    if hasattr(graph_obj, "builder") or isinstance(graph_obj, SecureCompiledGraph):
        is_valid_graph_primitive = True

    if not is_valid_graph_primitive:
        raise TypeError(
            f"The object found at '{module_path}' is not a valid compiled LangGraph workflow instance."
        )

    # 3. Hand the validated data straight down to your abstraction tier function
    insert_graph_registry(graph_key, module_path)


def register_app(thread_id: str, graph_key: str, expiration_timestamp: str) -> None:
    """Requests out-of-band SLA monitoring for an active application thread instance."""
    if not thread_id or not graph_key or not expiration_timestamp:
        raise ValueError(
            "thread_id, graph_key, and expiration_timestamp properties are all mandatory."
        )
    insert_sla_timer(thread_id, graph_key, expiration_timestamp)


def mark_thread_complete(thread_id: str) -> None:
    """Removes an active application thread from the background processing queue."""
    if not thread_id:
        raise ValueError(
            "A valid thread_id string is required to complete tracking tasks."
        )
    mark_sla_thread_complete(thread_id)
