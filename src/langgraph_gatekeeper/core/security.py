import inspect
from typing import Any, Callable, List

# Native framework configuration context primitive
from langgraph.config import get_config


# =============================================================================
# 1. THE ARCHITECTURAL INTERFACE CONTRACTS
# =============================================================================
class BaseSecurityPolicyProvider:
    """The Least Common Denominator Enterprise Security Boundary Interface."""

    def authorize(self, user_claims: List[str], resource: str, action: str) -> bool:
        raise NotImplementedError(
            "Concrete policy providers must implement the authorize method."
        )


class DefaultDictionaryPolicyProvider(BaseSecurityPolicyProvider):
    """The Library's Built-In Least-Friction Reference Wrapper."""

    def __init__(self, policy_matrix: dict):
        self.matrix = (
            policy_matrix  # Layout: {"node_name": {"required_claim": "claim_string"}}
        )

    def authorize(self, user_claims: List[str], resource: str, action: str) -> bool:
        node_rules = self.matrix.get(resource, {})
        required_claim = node_rules.get("required_claim")

        # If no explicit access restriction is mapped for this node, clear it flatly
        if not required_claim:
            return True

        return required_claim in user_claims


# =============================================================================
# 2. THE FLUENT COMPILER GRAPH BUILDER PROXY
# =============================================================================
class SecureGraphBuilder:
    """A fluent, chainable wrapper factory object that dynamically configures

    static entry gates while delegating execution queries to the compiled graph.
    """

    def __init__(self, compiled_graph: Any, policy_matrix: dict):
        self._compiled_graph = compiled_graph
        self._matrix = policy_matrix

    def enforce_entry(
        self, node_name: str, required_claim: str
    ) -> "SecureGraphBuilder":
        """Chains a baseline entry claim rule directly to a target graph node."""
        # Dynamically populate our internal memory structure out-of-band
        self._matrix[node_name] = {"required_claim": required_claim}
        return self

    def __getattr__(self, name: str) -> Any:
        # Pass through all standard graph methods (.get_state, .stream, etc.) unhindered
        return getattr(self._compiled_graph, name)


def compile_graph_with_authorization(
    workflow: Any, **kwargs: Any
) -> SecureGraphBuilder:
    """An architectural firewall that loops over canvas nodes right before compilation
    and returns a chainable SecureGraphBuilder to enforce entry gates strictly.
    """
    from langgraph_gatekeeper.core.task_cache_db import get_token_by_business_context

    policy_matrix = {}
    active_provider = DefaultDictionaryPolicyProvider(policy_matrix)

    # Internal safe dispatcher to execute both raw Python functions and compiled Runnables
    def _dispatch_runnable(runnable_obj: Any, *a: Any, **kw: Any) -> Any:
        if hasattr(runnable_obj, "invoke") and callable(runnable_obj.invoke):
            return runnable_obj.invoke(*a, **kw)
        return runnable_obj(*a, **kw)

    for node_name in list(workflow.nodes.keys()):
        node_spec = workflow.nodes[node_name]
        original_runnable = getattr(
            node_spec, "runnable", getattr(node_spec, "action", node_spec)
        )

        def create_secure_closure(node_key: str, orig_runnable: Any) -> Callable:

            def pre_execution_security_guard(*args: Any, **kwargs: Any) -> Any:
                # -----------------------------------------------------------------
                # NATIVE EXCEPTION-BOUND EXTRACTION
                # -----------------------------------------------------------------
                try:
                    config_dict = get_config() or {}
                except Exception:
                    return _dispatch_runnable(orig_runnable, *args, **kwargs)

                if not isinstance(config_dict, dict):
                    config_dict = getattr(
                        config_dict, "config", getattr(config_dict, "__dict__", {})
                    )

                configurable = (
                    config_dict.get("configurable", {})
                    if isinstance(config_dict, dict)
                    else {}
                )

                # High-Accuracy Execution Namespace String Extraction
                ns_string = configurable.get("checkpoint_ns") or ""
                if not ns_string and "__pregel_runtime" in configurable:
                    runtime = configurable["__pregel_runtime"]
                    exec_info = getattr(runtime, "execution_info", None)
                    ns_string = getattr(exec_info, "checkpoint_ns", "") or ""

                active_executing_node = ns_string.split(":")[0] if ns_string else None

                # If this frame belongs to an alternative node lane, bypass checking safely
                if active_executing_node and active_executing_node != node_key:
                    return _dispatch_runnable(orig_runnable, *args, **kwargs)

                # Fail-open system pass-through for global checkpointer updates (empty namespace strings)
                if ns_string == "":
                    return _dispatch_runnable(orig_runnable, *args, **kwargs)

                # -----------------------------------------------------------------
                # MUTUALLY EXCLUSIVE TWO-CLASS PERMISSION FIREWALL
                # -----------------------------------------------------------------
                user_id = configurable.get("user_id", "anonymous_user")
                user_claims = configurable.get("user_claims", [])
                thread_id = configurable.get("thread_id", "anonymous_thread")

                # Extract the un-spoofable action parameter injected by our orchestration entry points
                active_action = configurable.get("active_action", "execute")

                if active_action == "resume":
                    # -------------------------------------------------------------
                    # CLASS 2: DYNAMIC RESUMPTION HURDLE PROTECTION
                    # -------------------------------------------------------------
                    # Extract the threaded dynamic business context identifier cleanly from the envelope
                    biz_ctx = configurable.get(
                        "active_business_context", "default_context"
                    )

                    # Query the active database ledger out-of-band to recover the hurdle requirement
                    token_data = get_token_by_business_context(thread_id, biz_ctx)
                    required_claim = (
                        token_data.get("required_claim") if token_data else ""
                    )

                    # Enforce the strict dynamic hurdle permission check strictly!
                    if required_claim and required_claim not in user_claims:
                        raise PermissionError(
                            f"SECURITY INTERCEPTION: User '{user_id}' denied resumption access to node '{node_key}'. "
                            f"Missing required dynamic hurdle claim '{required_claim}'."
                        )
                else:
                    # -------------------------------------------------------------
                    # CLASS 1: BASELINE STATIC NODE ENTRY SECURITY CHECK
                    # -------------------------------------------------------------
                    is_authorized_entry = active_provider.authorize(
                        user_claims=user_claims, resource=node_key, action="execute"
                    )

                    if not is_authorized_entry:
                        raise PermissionError(
                            f"SECURITY INTERCEPTION: User '{user_id}' denied access to node '{node_key}'."
                        )

                return _dispatch_runnable(orig_runnable, *args, **kwargs)

            orig_name = getattr(
                getattr(original_runnable, "func", original_runnable),
                "__name__",
                node_key,
            )
            pre_execution_security_guard.__name__ = orig_name

            try:
                pre_execution_security_guard.__signature__ = inspect.signature(
                    original_runnable
                )
            except (ValueError, TypeError):
                if hasattr(original_runnable, "invoke"):
                    pre_execution_security_guard.__signature__ = inspect.signature(
                        original_runnable.invoke
                    )

            return pre_execution_security_guard

        secure_closure = create_secure_closure(node_name, original_runnable)

        if hasattr(node_spec, "runnable"):
            node_spec.runnable = secure_closure
        elif hasattr(node_spec, "action"):
            node_spec.action = secure_closure
        else:
            workflow.nodes[node_name] = secure_closure

    compiled_graph = workflow.compile(**kwargs)
    return SecureGraphBuilder(compiled_graph, policy_matrix)
