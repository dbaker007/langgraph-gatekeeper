from typing import Any

from langgraph.graph import MessagesState


class GatekeeperState(MessagesState, total=False):
    """The unified framework state abstraction tier tracking channel bounds."""

    user_id: str
    user_claims: list


class GatekeeperStateProxy(dict):
    """A dictionary-inheriting runtime proxy object providing seamless dot-notation
    access over LangGraph's raw flattened dictionary state payloads.
    """

    def __init__(self, target_dict: dict) -> None:
        super().__init__(target_dict)
        object.__setattr__(self, "_underlying_dict", target_dict)

    def __getattr__(self, name: str) -> Any:
        """Proxies dot-notation read lookups straight to the dictionary keys
        with graceful fallback to prevent node attribute execution crashes.
        """
        underlying = object.__getattribute__(self, "_underlying_dict")
        if name in underlying:
            return underlying[name]
        if name in self:
            return self[name]
        # Symmetrical fallback match to support uninitialized channel states
        return None

    def __setattr__(self, name: str, value: Any) -> None:
        """Proxies dot-notation writes straight into the dictionary keys."""
        underlying = object.__getattribute__(self, "_underlying_dict")
        underlying[name] = value
        self[name] = value

    def __delattr__(self, name: str) -> None:
        """Proxies dot-notation deletions straight from the dictionary keys."""
        underlying = object.__getattribute__(self, "_underlying_dict")
        if name in underlying:
            del underlying[name]
        if name in self:
            del self[name]
