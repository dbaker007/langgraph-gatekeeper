from typing import Any, Dict, List

from langgraph_gatekeeper.core.compiler import compile_secure_graph
from langgraph_gatekeeper.core.graph import SecureCompiledGraph


class SecureWorkflowGateway:
    """A micro-sized configuration entry class for the security framework.

    Tracks fine-grained node permission rules and hands them to the standalone
    compiler module at graph build time.
    """

    def __init__(self) -> None:
        self._policy_matrix: Dict[str, Dict[str, List[str]]] = {}

    def enforce_entry(
        self, node_name: str, required_claim: str
    ) -> "SecureWorkflowGateway":
        """Fluently configures static entry gate permissions directly on a graph canvas node."""
        if node_name not in self._policy_matrix:
            self._policy_matrix[node_name] = {"execute": []}

        if (
            required_claim
            and required_claim not in self._policy_matrix[node_name]["execute"]
        ):
            self._policy_matrix[node_name]["execute"].append(required_claim)
        return self

    def compile(self, workflow: Any, **kwargs: Any) -> SecureCompiledGraph:
        """Bakes the final secure graph asset by routing configuration properties

        down to the dedicated standalone compiler module.
        """
        return compile_secure_graph(workflow, self._policy_matrix, **kwargs)
