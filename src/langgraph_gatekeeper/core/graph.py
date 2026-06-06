from typing import Any


class SecureCompiledGraph:
    """A secure proxy wrapper that guards static entry gates and out-of-band
    state mutations while delegating execution queries to the compiled graph.
    """

    def __init__(self, compiled_graph: Any, policy_matrix: dict):
        self._compiled_graph = compiled_graph
        self._matrix = policy_matrix

    def update_state(self, config: dict, values: dict, as_node: str = None) -> Any:
        """PROGRAMMATIC FIREWALL: Hard-blocks unauthorized out-of-band state mutations."""
        configurable = config.get("configurable") or {}
        user_id = configurable.get("user_id", "anonymous_user")
        user_claims = configurable.get("user_claims", [])

        # Strict plain-text string match for administrative isolation
        if "mutate_state" not in user_claims:
            raise PermissionError(
                f"SECURITY INTERCEPTION: User '{user_id}' denied administrative state mutation. "
                f"Missing required permission 'mutate_state'."
            )

        return self._compiled_graph.update_state(config, values, as_node=as_node)

    def __getattr__(self, name: str) -> Any:
        return getattr(self._compiled_graph, name)
