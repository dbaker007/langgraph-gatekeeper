import importlib
import sys
import time
from typing import Any, Optional

from ttl_monitor.services import mark_thread_complete
from ttl_monitor.sla_db import (
    get_expired_sla_rows,
    get_graph_module_path,
)


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
            # Forcefully landing onto the empty Node 1 checkpointer anchor row
            graph_instance.update_state(
                config={"configurable": {"thread_id": thread_id}},
                values={"kill_switch_reason": "SLA_TIMEOUT_EXPIRED"},
                as_node="kill_switch",
            )

            # The engine turns and naturally steps down the canvas edge into Node 2!
            list(
                graph_instance.stream(
                    None, config={"configurable": {"thread_id": thread_id}}
                )
            )

            mark_thread_complete(thread_id)
            eviction_count += 1

        except Exception:
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
