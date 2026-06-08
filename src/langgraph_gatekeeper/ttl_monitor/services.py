import importlib
import sqlite3

from langgraph_gatekeeper.core.graph import SecureCompiledGraph
from langgraph_gatekeeper.ttl_monitor.sla_db import (
    SLA_DB_PATH,
    init_sla_db,
    insert_graph_registry,
    insert_sla_timer,
    mark_sla_thread_complete,
)


def clear_infrastructure_registry_tables() -> None:
    """Service Utility: Truncates and resets local tracking databases out-of-band."""
    init_sla_db()
    with sqlite3.connect(SLA_DB_PATH, timeout=30.0) as conn:
        conn.execute("DELETE FROM sla_timers")
        conn.execute("DELETE FROM graph_registry")
        conn.commit()


def register_graph_asset(graph_key: str, module_path: str) -> None:
    """Registers a compiled graph microservice reference path into the background daemon registry."""
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
            f"The module '{mod_name}' does not possess attribute '{attr_name}'."
        )

    graph_obj = getattr(mod, attr_name)

    is_valid_graph_primitive = False
    if (
        hasattr(graph_obj, "builder")
        or isinstance(graph_obj, SecureCompiledGraph)
        or hasattr(graph_obj, "graph")
    ):
        is_valid_graph_primitive = True

    if not is_valid_graph_primitive:
        raise TypeError(
            f"The object found at '{module_path}' is not a valid compiled LangGraph workflow instance."
        )

    insert_graph_registry(graph_key, module_path)


def register_app(thread_id: str, graph_key: str, expiration_timestamp: str) -> None:
    """Requests out-of-band SLA monitoring for an active application thread instance."""
    if not thread_id or not graph_key or not expiration_timestamp:
        raise ValueError("All parameters are mandatory.")
    insert_sla_timer(thread_id, graph_key, expiration_timestamp)


def mark_thread_complete(thread_id: str) -> None:
    """Removes an active application thread from the background processing queue."""
    if not thread_id:
        raise ValueError("A valid thread_id string is required.")
    mark_sla_thread_complete(thread_id)
