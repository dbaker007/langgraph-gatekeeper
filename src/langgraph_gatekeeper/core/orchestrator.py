from typing import Any, Dict, Generator

from langgraph.types import Command

# Import the native primitive hidden out-of-band to drive our custom wrapper
from langgraph.types import interrupt as langgraph_native_interrupt

from langgraph_gatekeeper.core.task_cache_db import (
    delete_active_task_token,
    get_active_task_token,
    init_task_cache_db,
    save_active_task_token,
)


def interrupt(payload: Any) -> Any:
    """The Framework's Custom Interrupt Wrapper.

    Exposes a unified, clean contract to the application layer while
    driving the underlying framework primitives natively behind the scenes.
    """
    # Simply forward the payload down to LangGraph's native execution engine hurdle
    return langgraph_native_interrupt(payload)


def execute_graph(
    graph: Any, inputs: Dict[str, Any], config: Dict[str, Any]
) -> Generator[Dict[str, Any], None, None]:
    """Launches an initial workflow thread pass and harvests volatile framework tokens out-of-band."""
    init_task_cache_db()

    # Turn the core framework execution engine crank
    stream = graph.stream(inputs, config=config)

    for event in stream:
        # Intercept native framework inline interrupt milestones out-of-band
        if "__interrupt__" in event:
            active_interrupts = event["__interrupt__"]
            for item in active_interrupts:
                if isinstance(item, tuple) and len(item) > 0:
                    item = item[0]

                # Extract the application-layer target routing key string name
                target_key = getattr(item, "value", {}).get("routing_key", "")
                interrupt_id = getattr(item, "id", None)

                if target_key and interrupt_id:
                    # Cache the token mapping relationship inside the staging infrastructure tier
                    save_active_task_token(target_key, interrupt_id)

        yield event


def resume(
    graph: Any, routing_key: str, user_input: Any, config: Dict[str, Any]
) -> Generator[Dict[str, Any], None, None]:
    """Automates token matching to seamlessly wake up a frozen parallel canvas branch.

    Delays task token database deletion until the caller context successfully clears
    the framework's pre-execution security firewall boundaries.
    """
    init_task_cache_db()
    token_id = get_active_task_token(routing_key)

    if not token_id:
        raise ValueError(
            f"No active interrupt token found matching routing key '{routing_key}'."
        )

    # Materialize the underlying framework streaming runtime generator loop
    stream = graph.stream(Command(resume={token_id: user_input}), config=config)

    # -----------------------------------------------------------------
    # TRANSACTION-SAFE DELAYED DELETION LOOP
    # -----------------------------------------------------------------
    iterator = iter(stream)

    try:
        first_event = next(iterator)
    except StopIteration:
        delete_active_task_token(routing_key)
        return

    # SUCCESS! The stream moved past the security gate without raising a PermissionError.
    delete_active_task_token(routing_key)

    # Yield the validated first event and stream the remaining cascade natively
    yield first_event
    for event in iterator:
        yield event
