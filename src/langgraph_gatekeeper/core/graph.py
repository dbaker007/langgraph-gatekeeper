from typing import Any, Optional


class SecureCompiledGraph:
    """A centralized security proxy wrapping LangGraph's CompiledGraph asset

    to enforce programmatic firewalls around dynamic checkpoint alterations.
    """

    def __init__(self, compiled_graph: Any, policy_matrix: dict) -> None:
        self.graph = compiled_graph
        self.policy_matrix = policy_matrix

    def update_state(
        self, config: dict, values: dict, as_node: Optional[str] = None
    ) -> Any:
        """PROGRAMMATIC FIREWALL: Hard-blocks unauthorized out-of-band state mutations."""
        configurable = config.get("configurable") or {}
        user_id = configurable.get("user_id", "anonymous_user")
        user_claims = configurable.get("user_claims") or []

        if "mutate_state" not in user_claims:
            raise PermissionError(
                f"SECURITY INTERCEPTION: User '{user_id}' denied administrative state mutation. "
                f"Missing required permission 'mutate_state'."
            )

        return self.graph.update_state(config, values, as_node=as_node)

    def get_state(self, config: dict) -> Any:
        """Proxies state snapshot reads natively down to the inner graph."""
        return self.graph.get_state(config)

    def get_state_history(self, config: dict) -> Any:
        """Proxies thread state history traversal natively down to the inner graph."""
        return self.graph.get_state_history(config)

    @property
    def stream(self) -> Any:
        """Proxies graph stream ticks natively down to the inner graph."""
        return self.graph.stream
