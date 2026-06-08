from typing import Any, List

from langgraph.graph import MessagesState
from pydantic import BaseModel, Field


class GatekeeperMessagesState(MessagesState, total=False):  # type: ignore[call-arg]
    """A dictionary-based (TypedDict) high-assurance base schema layout.

    Automatically pre-injects required security metrics and identity tracking channels.
    """

    user_id: str
    user_claims: List[str]


class GatekeeperObjectState(BaseModel):
    """An object-oriented (Pydantic) high-assurance base schema model wrapper.

    Provides traditional class structures and auto-complete hints natively.
    """

    user_id: str = Field(default="anonymous_user")
    user_claims: List[str] = Field(default_factory=list)
    messages: List[Any] = Field(default_factory=list)


class GatekeeperStateProxy(dict):
    """An optimized, loop-free data routing lens proxy offering safe dot-notation

    property access across flattened dictionary channels.
    """

    def __init__(self, target_dict: dict) -> None:
        super().__init__(target_dict)

    def __getattr__(self, name: str) -> Any:
        if super().__contains__(name):
            return super().__getitem__(name)
        return None

    def __setattr__(self, name: str, value: Any) -> None:
        super().__setitem__(name, value)

    def __delattr__(self, name: str) -> None:
        if super().__contains__(name):
            super().__delitem__(name)
        else:
            raise AttributeError(f"'GatekeeperStateProxy' has no attribute '{name}'")

    def __bool__(self) -> bool:
        return len(self) > 0
