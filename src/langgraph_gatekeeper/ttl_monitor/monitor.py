import importlib
import sys
import time
from typing import Any, Dict, Optional

from langgraph_gatekeeper.ttl_monitor.services import mark_thread_complete
from langgraph_gatekeeper.ttl_monitor.sla_db import (
    get_expired_sla_rows,
    get_graph_module_path,
)

_DAEMON_IDENTITY_CONTEXT: Dict[str, Any] = {
    "user_id": "anonymous_daemon_worker",
    "user_claims": [],
}


def set_framework_daemon_identity(identity_context: Dict[str, Any]) -> None:
    global _DAEMON_IDENTITY_CONTEXT
    if not isinstance(identity_context, dict):
        raise TypeError("The identity_context must be a valid dictionary payload.")
    _DAEMON_IDENTITY_CONTEXT = identity_context


def resolve_graph_by_key(graph_key: str) -> Optional[Any]:
    module_path = get_graph_module_path(graph_key)
    if not module_path:
        return None

    try:
        module_name, variable_name = module_path.split(":")
        module = importlib.import_module(module_name)
        return getattr(module, variable_name)
    except (ImportError, AttributeError, ValueError):
        return None


def run_ttl_monitor_cycle() -> int:
    """Scans the infrastructure storage tier for expired application SLA deadlines

    and forcefully ejects breached threads using out-of-band state evictions.
    """
    expired_rows = get_expired_sla_rows()
    eviction_count = 0

    for row in expired_rows:
        thread_id = row["thread_id"]
        graph_key = row["graph_key"]

        graph_instance = resolve_graph_by_key(graph_key)
        if not graph_instance:
            continue

        try:
            # Your exact original config layout, carrying the registered service capabilities
            daemon_config = {
                "configurable": {
                    "thread_id": thread_id,
                    "user_id": _DAEMON_IDENTITY_CONTEXT.get(
                        "user_id", "anonymous_daemon_worker"
                    ),
                    "user_claims": _DAEMON_IDENTITY_CONTEXT.get("user_claims", []),
                }
            }

            # Your exact original update_state pass
            graph_instance.update_state(
                config=daemon_config,
                values={"kill_switch_reason": "SLA_TIMEOUT_EXPIRED"},
                as_node="kill_switch",
            )

            # Your exact original streaming loop driver. Passes None to drive the checkpoint naturally.
            list(graph_instance.stream(None, config=daemon_config))

            mark_thread_complete(thread_id)
            eviction_count += 1

        except Exception as e:
            import traceback

            print(f"\n[WAM DAEMON CRASH DETECTED]: {e}")
            traceback.print_exc()
            continue

    return eviction_count


if __name__ == "__main__":
    print("[SLA DAEMON] Initializing background TTL polling monitor process...")
    while True:
        try:
            processed = run_ttl_monitor_cycle()
            if processed > 0:
                print(
                    f"[SLA DAEMON] Successfully evicted {processed} breached threads."
                )
        except Exception as daemon_err:
            print(f"[SLA DAEMON CRITICAL ERROR]: {daemon_err}", file=sys.stderr)

        time.sleep(1.0)
