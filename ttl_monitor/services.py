import importlib
import sqlite3

from core.security import verify_registration_security_policy
from ttl_monitor.sla_db import (
    SLA_DB_PATH,
    init_sla_db,
    insert_graph_registry,
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
    """Validates and registers an application graph path string to the database."""
    if not graph_key or not module_path:
        raise ValueError("Both graph_key and module_path strings must be provided.")

    if ":" not in module_path:
        raise ValueError("The module_path format must use a colon separator.")

    verify_registration_security_policy(graph_key)

    try:
        module_name, variable_name = module_path.split(":")
        module = importlib.import_module(module_name)
        graph_instance = getattr(module, variable_name)
    except (ImportError, AttributeError) as err:
        raise ValueError(
            f"Failed to dynamically load graph asset path '{module_path}'. {err}"
        )

    if not hasattr(graph_instance, "nodes") or not isinstance(
        graph_instance.nodes, dict
    ):
        raise TypeError(
            f"The object found at '{module_path}' is not a valid compiled LangGraph workflow instance."
        )

    if "kill_switch" not in graph_instance.nodes:
        raise KeyError(
            f"CRITICAL REGISTRATION REFUSAL: Missing mandatory 'kill_switch' node canvas registration."
        )

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
