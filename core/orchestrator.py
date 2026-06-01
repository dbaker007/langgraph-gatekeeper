from typing import Any, Dict, Generator, List

from langgraph.types import Command
from langgraph.types import interrupt as langgraph_interrupt
from pydantic import BaseModel, ConfigDict, Field

from core.task_cache_db import (
    delete_active_task_token,
    get_active_task_token,
    init_task_cache_db,
    save_active_task_token,
)


class InterruptPayload(BaseModel):
    routing_key: str = Field(
        ...,
        description="The globally unique transaction identifier generated dynamically by the node.",
    )
    model_config = ConfigDict(extra="allow")


def _intercept_active_tokens(events: List[Dict[str, Any]]) -> None:
    """Scans stream event updates to record volatile framework task tokens."""
    init_task_cache_db()
    for event in events:
        if "__interrupt__" in event:
            active_interrupts = event["__interrupt__"]
            for item in active_interrupts:
                target_key = None
                payload = item.value

                if isinstance(payload, dict):
                    target_key = next(
                        (
                            v
                            for k, v in payload.items()
                            if isinstance(v, str) and k != "prompt"
                        ),
                        None,
                    )
                elif hasattr(payload, "__dict__"):
                    target_key = next(
                        (
                            v
                            for k, v in payload.__dict__.items()
                            if isinstance(v, str) and k != "prompt"
                        ),
                        None,
                    )

                if target_key:
                    save_active_task_token(target_key, item.id)


def interrupt(payload: InterruptPayload) -> Any:
    return langgraph_interrupt(payload)


def execute_graph(
    graph: Any, input_data: Dict[str, Any] | Any, config: Dict[str, Any]
) -> Generator[Dict[str, Any], None, None]:
    """Launches a fresh workflow run stream turn, harvesting generated telemetry tokens."""
    events = list(graph.stream(input_data, config, stream_mode="updates"))
    _intercept_active_tokens(events)
    yield from events


def resume(
    graph: Any, routing_key: str, user_input: Any, config: Dict[str, Any]
) -> Generator[Dict[str, Any], None, None]:
    """Automates token matching to seamlessly wake up a frozen parallel canvas branch."""
    token_id = get_active_task_token(routing_key)
    if not token_id:
        raise ValueError(
            f"No active interrupt token found matching routing key '{routing_key}'."
        )

    delete_active_task_token(routing_key)
    resume_command = Command(resume={token_id: user_input})

    # Pass the clean, standard business config natively to avoid internal checkpoint conflicts
    events = list(graph.stream(resume_command, config, stream_mode="updates"))
    _intercept_active_tokens(events)
    yield from events
