from typing import Any


class SecureCompiledGraph:
    """A centralized security proxy wrapping LangGraph's CompiledGraph asset

    to enforce programmatic firewalls around dynamic checkpoint alterations.
    """

    def __init__(self, compiled_graph: Any, policy_matrix: dict) -> None:
        self.graph = compiled_graph
        self.policy_matrix = policy_matrix

    def update_state(self, config: dict, values: dict, as_node: str = None) -> Any:
        """PROGRAMMATIC FIREWALL: Hard-blocks unauthorized out-of-band state mutations

        while automatically formatting message histories to ensure LangGraph compliance.
        """
        from langchain_core.messages import AIMessage

        configurable = config.get("configurable") or {}
        user_id = configurable.get("user_id", "anonymous_user")
        user_claims = configurable.get("user_claims", [])

        if "mutate_state" not in user_claims:
            raise PermissionError(
                f"SECURITY INTERCEPTION: User '{user_id}' denied administrative state mutation. "
                f"Missing required permission 'mutate_state'."
            )

        # AUTOMATED HISTORY SYNC: If the developer is injecting pending tool calls out-of-band,
        # automatically manufacture the compliant AIMessage history payload channel for them!
        if (
            isinstance(values, dict)
            and "pending_tool_calls" in values
            and values["pending_tool_calls"]
        ):
            tool_calls = values["pending_tool_calls"]
            values["messages"] = [AIMessage(content="", tool_calls=tool_calls)]

        # FIXED: Core tracking pointer renamed back to self.graph natively
        return self.graph.update_state(config, values, as_node=as_node)

    def get_state(self, config: dict) -> Any:
        return self.graph.get_state(config)

    def get_state_history(self, config: dict) -> Any:
        return self.graph.get_state_history(config)

    @property
    def stream(self) -> Any:
        return self.graph.stream
