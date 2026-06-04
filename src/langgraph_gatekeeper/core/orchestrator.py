from typing import Any, Dict, Generator, Optional

from langgraph.types import Command
from langgraph.types import interrupt as langgraph_native_interrupt

# FIXED: Removed the undefined get_thread_id_by_routing_key from the import array!
from langgraph_gatekeeper.core.task_cache_db import (
    consume_active_task_token,
    get_active_task_token,
    get_token_by_business_context,
    init_task_cache_db,
    save_active_task_token,
)


def interrupt(routing_key: str, business_context: str, payload: dict) -> Any:
    """THE FRAMEWORK'S CUSTOM INTERRUPT CONTRACT WRAPPER.

    Enforces a strict parameter structure requiring application tool builders to
    provide an explicit business domain context alongside their routing keys.
    """
    enriched_payload = {
        "routing_key": routing_key,
        "business_context": business_context,
        **payload,
    }
    return langgraph_native_interrupt(enriched_payload)


def execute_graph(
    graph: Any, inputs: Dict[str, Any], config: Dict[str, Any]
) -> Generator[Dict[str, Any], None, None]:
    """Launches an initial workflow thread pass and harvests thread tracking tokens out-of-band."""
    init_task_cache_db()

    thread_id = config.get("configurable", {}).get("thread_id", "anonymous_thread")

    stream = graph.stream(inputs, config=config)
    for event in stream:
        if "__interrupt__" in event:
            active_interrupts = event["__interrupt__"]
            for item in active_interrupts:
                if isinstance(item, tuple) and len(item) > 0:
                    item = item

                inner_payload = getattr(item, "value", {}) or {}
                if isinstance(inner_payload, dict):
                    target_key = inner_payload.get("routing_key", "")
                    biz_ctx = inner_payload.get("business_context", "default_context")
                else:
                    target_key = ""
                    biz_ctx = "default_context"

                interrupt_id = getattr(item, "id", None)

                if target_key and interrupt_id:
                    save_active_task_token(
                        target_key, interrupt_id, thread_id, business_context=biz_ctx
                    )

        yield event


def resume(
    graph: Any, routing_key: str, user_input: Any, config: Dict[str, Any]
) -> Generator[Dict[str, Any], None, None]:
    """LEGACY HOOK: Direct Instance Routing Key Matching.

    Maintains 100% backward compatibility for existing application code paths.
    """
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

    consume_active_task_token(routing_key)

    yield first_event
    for event in iterator:
        yield event


def resume_by_context(
    graph: Any, business_context: str, user_input: Any, config: Dict[str, Any]
) -> Generator[Dict[str, Any], None, None]:
    """NEW COMPOSITE HOOK: Context-Aware Enterprise Resumption.

    Wakes up a frozen thread utilizing exactly what the resumer knows at the moment of impact:
    the thread_id (extracted from config) and the specific business domain context.
    """
    init_task_cache_db()
    thread_id = config.get("configurable", {}).get("thread_id")

    if not thread_id:
        raise ValueError(
            "Resumption configuration context envelope lacks a valid thread_id pointer."
        )

    token_data = get_token_by_business_context(thread_id, business_context)
    if not token_data:
        raise ValueError(
            f"No active interrupt token located for composite primary key pair: "
            f"thread_id='{thread_id}' AND business_context='{business_context}'."
        )

    token_id = token_data["token_id"]
    target_routing_key = token_data["routing_key"]

    stream = graph.stream(Command(resume={token_id: user_input}), config=config)
    iterator = iter(stream)

    try:
        first_event = next(iterator)
    except StopIteration:
        consume_active_task_token(target_routing_key)
        return

    consume_active_task_token(target_routing_key)

    yield first_event
    for event in iterator:
        yield event


def get_historical_thread_status(graph: Any, config: Dict[str, Any]) -> Dict[str, Any]:
    """Exposes a clean facade method to recover business transaction metrics out-of-band."""
    init_task_cache_db()
    thread_id = config.get("configurable", {}).get("thread_id")

    if not thread_id:
        return {
            "status": "UNKNOWN",
            "detail": "Missing thread identification parameters.",
        }

    try:
        snapshot = graph.get_state(config)
        history = list(graph.get_state_history(config))
    except Exception as err:
        return {
            "status": "UNKNOWN",
            "detail": f"Failed to query checkpointer logs: {str(err)}",
        }

    if snapshot.next:
        return {
            "status": "PENDING",
            "thread_id": thread_id,
            "next_step": snapshot.next,
            "detail": "Workflow thread is frozen at an automated security hurdle awaiting supervisor intervention.",
        }

    manager_decision = "Processed"
    for state_frame in history:
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
