import inspect
from typing import Any, Callable, Dict, Set

# =============================================================================
# 1. THE DECOUPLED POLICY REGISTRY (PERMISSIONS, NOT ROLES)
# =============================================================================
NODE_PERMISSION_REGISTRY: Dict[str, Dict[str, str]] = {
    "assign_agent": {
        "initial": "execute_concierge_workflow",
    },
    "assign_credit": {
        "initial": "execute_credit_workflow",
        "resumption": "approve_vip_limit",  # Elevated action token constraint
    },
    "kill_switch": {"initial": "execute_platform_eviction"},
}

# The corporate Role-to-Permission Indirection Matrix
ROLE_PERMISSION_MATRIX: Dict[str, Set[str]] = {
    "basic_analyst": {"execute_credit_workflow", "execute_concierge_workflow"},
    "executive_underwriter": {
        "execute_credit_workflow",
        "execute_concierge_workflow",
        "approve_vip_limit",
        "execute_platform_eviction",
    },
}


def resolve_user_permissions(user_roles: list[str]) -> Set[str]:
    """Flattens and aggregates a user's combined permissions across all assigned roles."""
    aggregated_permissions: Set[str] = set()
    for role in user_roles:
        if role in ROLE_PERMISSION_MATRIX:
            aggregated_permissions |= ROLE_PERMISSION_MATRIX[role]
    return aggregated_permissions


def verify_registration_security_policy(graph_key: str) -> None:
    if "kill_switch" not in NODE_PERMISSION_REGISTRY:
        raise KeyError(
            "REGISTRATION FIREWALL BLOCK: The security policy matrix is missing an explicit "
            "permission assignment for the mandatory 'kill_switch' node boundary."
        )


# =============================================================================
# 2. AUTOMATED GRAPH COMPILATION INTERCEPTOR (ACCURATE CASCADE DETECTION)
# =============================================================================
def compile_graph_with_authorization(workflow: Any, **kwargs: Any) -> Any:
    """An architectural firewall that loops over canvas nodes right before compilation

    and injects a framework-level pre-execution security closure.
    """
    for node_name in list(workflow.nodes.keys()):
        if node_name in NODE_PERMISSION_REGISTRY:
            policy = NODE_PERMISSION_REGISTRY[node_name]
            node_spec = workflow.nodes[node_name]

            original_runnable = getattr(
                node_spec, "runnable", getattr(node_spec, "action", node_spec)
            )

            def create_secure_closure(
                node_key: str, node_policy: Dict[str, str], orig_runnable: Any
            ) -> Callable:

                def pre_execution_security_guard(*args: Any, **kwargs: Any) -> Any:
                    config = (
                        kwargs.get("config")
                        if "config" in kwargs
                        else (args if len(args) > 1 else None)
                    )
                    config_dict = (
                        config
                        if isinstance(config, dict)
                        else getattr(config, "config", {})
                    )
                    if not isinstance(config_dict, dict):
                        config_dict = getattr(config, "__dict__", {})

                    configurable = (
                        config_dict.get("configurable", {})
                        if isinstance(config_dict, dict)
                        else {}
                    )

                    user_id = configurable.get("user_id", "anonymous_user")
                    user_roles = configurable.get("user_roles", [])
                    active_permissions = resolve_user_permissions(user_roles)

                    # -----------------------------------------------------------------
                    # HIGH-ACCURACY TURN DETECTION
                    # -----------------------------------------------------------------
                    # LangGraph passes active inputs to the target node via '__pregel_resume_map'.
                    # A turn resolves as a resumption ONLY if this specific node is the one
                    # being targeted by the execution frame. Downstream steps running as a sequential
                    # cascade turn carry an empty task mapping state for that step execution.
                    runtime = configurable.get("__pregel_runtime")
                    is_resuming_flag = (
                        config_dict.get("__pregel_resuming") is True
                        or configurable.get("__pregel_resuming") is True
                    )

                    # If execution info is present, we assert if this specific step is the active target.
                    # If we are cascading to a NEW node, the framework executes it as a fresh initial run.
                    current_node = configurable.get("langgraph_node")

                    # A step is a resumption ONLY if the framework is actively executing the target node
                    # that received the resume command. Downstream steps evaluate as initial cascades.
                    is_resumption = is_resuming_flag and (current_node == node_key)

                    if is_resumption:
                        required_permission = node_policy.get(
                            "resumption", node_policy["initial"]
                        )
                        action_type = "RESUMPTION"
                    else:
                        required_permission = node_policy["initial"]
                        action_type = "INITIAL_EXECUTION"

                    if required_permission not in active_permissions:
                        raise PermissionError(
                            f"SECURITY INTERCEPTION: User '{user_id}' denied {action_type} access to node '{node_key}'. "
                            f"Missing mandatory operation permission string '{required_permission}'."
                        )

                    return orig_runnable.invoke(*args, **kwargs)

                orig_name = getattr(
                    getattr(orig_runnable, "func", orig_runnable), "__name__", node_key
                )
                pre_execution_security_guard.__name__ = orig_name

                pre_execution_security_guard.__signature__ = inspect.Signature(
                    [
                        inspect.Parameter(
                            "state", inspect.Parameter.POSITIONAL_OR_KEYWORD
                        ),
                        inspect.Parameter(
                            "config", inspect.Parameter.POSITIONAL_OR_KEYWORD
                        ),
                    ]
                )
                return pre_execution_security_guard

            secure_closure = create_secure_closure(node_name, policy, original_runnable)

            if hasattr(node_spec, "runnable"):
                node_spec.runnable = secure_closure
            elif hasattr(node_spec, "action"):
                node_spec.action = secure_closure
            else:
                workflow.nodes[node_name] = secure_closure

    return workflow.compile(**kwargs)
