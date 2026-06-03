from typing import Any, Dict, Generator, Optional

from langgraph.types import Command
from langgraph.types import interrupt as langgraph_native_interrupt

from langgraph_gatekeeper.core.task_cache_db import (
    consume_active_task_token,
    get_active_task_token,
    get_thread_id_by_routing_key,
    init_task_cache_db,
    save_active_task_token,
)


def interrupt(payload: Any) -> Any:
    """The Framework's Custom Interrupt Wrapper."""
    return langgraph_native_interrupt(payload)


def execute_graph(
    graph: Any, inputs: Dict[str, Any], config: Dict[str, Any]
) -> Generator[Dict[str, Any], None, None]:
    """Launches an initial workflow thread pass and harvests thread tracking tokens out-of-band."""
    init_task_cache_db()

    # Extract the foundational thread identifier from the active configuration envelope
    thread_id = config.get("configurable", {}).get("thread_id", "anonymous_thread")

    stream = graph.stream(inputs, config=config)
    for event in stream:
        if "__interrupt__" in event:
            active_interrupts = event["__interrupt__"]
            for item in active_interrupts:
                if isinstance(item, tuple) and len(item) > 0:
                    item = item[0]

                target_key = getattr(item, "value", {}).get("routing_key", "")
                interrupt_id = getattr(item, "id", None)

                if target_key and interrupt_id:
                    # FIXED: Cache the permanent thread_id mapping handle alongside the transient token
                    save_active_task_token(target_key, interrupt_id, thread_id)

        yield event


def resume(
    graph: Any, routing_key: str, user_input: Any, config: Dict[str, Any]
) -> Generator[Dict[str, Any], None, None]:
    """Automates token matching to wake up a frozen canvas branch while preserving history."""
    init_task_cache_db()
    token_id = get_active_task_token(routing_key)

    if not token_id:
        raise ValueError(
            f"No active interrupt token found matching routing key '{routing_key}'."
        )

    stream = graph.stream(Command(resume={token_id: user_input}), config=config)
    iterator = iter(stream)

    try:
        first_event = next(iterator)
    except StopIteration:
        consume_active_task_token(routing_key)
        return

    # SUCCESS! The caller cleared the firewall. Mark the token row as CONSUMED.
    consume_active_task_token(routing_key)

    yield first_event
    for event in iterator:
        yield event


def get_historical_thread_status(graph: Any, routing_key: str) -> Dict[str, Any]:
    """Exposes a clean facade method to recover business transaction metrics out-of-band.

    Resolves the hidden thread handle from our metadata registry, queries LangGraph's
    immutable event log checkpoints, and returns the historical state metrics.
    """
    init_task_cache_db()
    thread_id = get_thread_id_by_routing_key(routing_key)

    if not thread_id:
        return {
            "status": "UNKNOWN",
            "detail": f"No internal tracking history located for key '{routing_key}'.",
        }

    target_config = {"configurable": {"thread_id": thread_id}}

    try:
        snapshot = graph.get_state(target_config)
        history = list(graph.get_state_history(target_config))
    except Exception as err:
        return {
            "status": "UNKNOWN",
            "detail": f"Failed to query checkpointer logs: {str(err)}",
        }

    # Case 1: If there are active downstream targets pending, the workflow is currently frozen
    if snapshot.next:
        return {
            "status": "PENDING",
            "thread_id": thread_id,
            "next_step": snapshot.next[0],
            "detail": "Workflow thread is frozen at an automated security hurdle awaiting supervisor intervention.",
        }

    # Case 2: Read the event ledger backwards to discover the manager's resolution text payload
    manager_decision = "Processed"
    for state_frame in history:
        # Inspect if a Command resume action payload was injected during this historical state turn
        # In standard LangGraph history checkpoints, metadata or values record previous inputs
        if hasattr(state_frame, "values") and isinstance(state_frame.values, dict):
            res_val = state_frame.values.get("result_data") or state_frame.values.get(
                "payload"
            )
            if res_val:
                manager_decision = str(res_val)
                break

    return {
        "status": "PROCESSED",
        "thread_id": thread_id,
        "resolution": manager_decision,
        "detail": f"Workflow completed cleanly. Transaction resolved with signature: '{manager_decision}'.",
    }
