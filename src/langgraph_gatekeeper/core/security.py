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
            policy_matrix  # Layout: {"node_name": {"action_verb": "required_claim"}}
        )

    def authorize(self, user_claims: List[str], resource: str, action: str) -> bool:
        resource_rules = self.matrix.get(resource, {})
        required_claim = resource_rules.get(action)

        # If no explicit access restriction is mapped for this action verb, clear it flatly
        if not required_claim:
            return True

        return required_claim in user_claims


# =============================================================================
# 2. AUTOMATED GRAPH COMPILATION INTERCEPTOR (CONTEXTVAR BOUNDED)
# =============================================================================
def compile_graph_with_authorization(
    workflow: Any, policy_provider: Any, **kwargs: Any
) -> Any:
    """An architectural firewall that loops over canvas nodes right before compilation

    and injects a framework-level pre-execution security closure.
    """
    if isinstance(policy_provider, dict):
        active_provider = DefaultDictionaryPolicyProvider(policy_provider)
    else:
        active_provider = policy_provider

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
                # Pull the active streaming configuration context directly from LangGraph's
                # global context variable container out-of-band.
                try:
                    config_dict = get_config() or {}
                except Exception:
                    # get_config() failed because LangGraph is running an out-of-band
                    # compile, setup, or definition loop. Pass through instantly unhindered.
                    return orig_runnable.invoke(*args, **kwargs)

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
                    return orig_runnable.invoke(*args, **kwargs)

                # -----------------------------------------------------------------
                # STRICT CLAIMS RUNTIME FIREWALL
                # -----------------------------------------------------------------
                user_id = configurable.get("user_id", "anonymous_user")
                user_claims = configurable.get("user_claims", [])

                # High-Accuracy Turn Decoding independent of history string structures
                is_resuming_flag = (
                    config_dict.get("__pregel_resuming") is True
                    or configurable.get("__pregel_resuming") is True
                )
                is_resumption = is_resuming_flag and (active_executing_node == node_key)
                action_verb = "approve" if is_resumption else "execute"

                is_authorized = active_provider.authorize(
                    user_claims=user_claims, resource=node_key, action=action_verb
                )

                if not is_authorized:
                    raise PermissionError(
                        f"SECURITY INTERCEPTION: User '{user_id}' denied access to node '{node_key}'."
                    )

                return orig_runnable.invoke(*args, **kwargs)

            orig_name = getattr(
                getattr(original_runnable, "func", original_runnable),
                "__name__",
                node_key,
            )
            pre_execution_security_guard.__name__ = orig_name
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

    return workflow.compile(**kwargs)
