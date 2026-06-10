import inspect
from typing import Any, Callable

from langgraph.config import get_config

from langgraph_gatekeeper.core.graph import SecureCompiledGraph
from langgraph_gatekeeper.core.models import GatekeeperObjectState


def validate_workflow_schema(workflow: Any) -> None:
    """Enforces type-safe schema contract compliance by inspecting the workflow directly.

    Validates that a custom canvas state schema properly subclasses or adheres
    to the framework's strict identity contract specifications.
    """
    is_valid_schema = False
    schema_obj = getattr(workflow, "state_schema", None)

    if schema_obj is not None and isinstance(schema_obj, type):
        # 1. OPTIMIZED FAST PATH: Inspect traditional Pydantic object state ancestry first
        if issubclass(schema_obj, GatekeeperObjectState):
            is_valid_schema = True
        else:
            # 2. FALLBACK PATH: Inspect dictionary or class-style state schemas via annotations
            annotations = getattr(schema_obj, "__annotations__", {})
            if "user_id" in annotations and "user_claims" in annotations:
                is_valid_schema = True

    if not is_valid_schema:
        raise TypeError(
            "CRITICAL COMPILATION REJECTION: Your StateGraph schema configuration must "
            "explicitly declare the 'user_id' and 'user_claims' identity tracking channels, "
            "or subclass GatekeeperObjectState."
        )


def compile_secure_graph(
    workflow: Any, policy_matrix: dict, **kwargs: Any
) -> SecureCompiledGraph:
    """An optimized standalone compiler module that intercepts graph canvas nodes
    to inject zero-trust boundary verification guards natively.
    """
    # Invoke the standalone type-safe schema contract validator directly on the workflow
    validate_workflow_schema(workflow)

    def _dispatch_runnable(runnable_obj: Any, *a: Any, **kw: Any) -> Any:
        if hasattr(runnable_obj, "invoke") and callable(runnable_obj.invoke):
            return runnable_obj.invoke(*a, **kw)
        return runnable_obj(*a, **kw)

    canvas_node_keys = list(workflow.nodes.keys())

    for node_name in canvas_node_keys:
        node_spec = workflow.nodes[node_name]
        original_runnable = getattr(
            node_spec, "runnable", getattr(node_spec, "action", node_spec)
        )

        def create_secure_closure(
            node_key: str, orig_runnable: Any, full_policy: dict
        ) -> Callable:
            def pre_execution_security_guard(*args: Any, **kwargs: Any) -> Any:
                from langgraph_gatekeeper.core.models import GatekeeperStateProxy

                args_list = list(args)
                if (
                    args_list
                    and hasattr(args_list, "get")
                    and isinstance(args_list, dict)
                ):
                    args_list = GatekeeperStateProxy(args_list)
                args = tuple(args_list)

                try:
                    config_dict = get_config() or {}
                except Exception:
                    config_dict = {}

                if not isinstance(config_dict, dict):
                    config_dict = getattr(
                        config_dict, "config", getattr(config_dict, "__dict__", {})
                    )

                configurable = (
                    config_dict.get("configurable", {})
                    if isinstance(config_dict, dict)
                    else {}
                )

                user_id = configurable.get("user_id", "anonymous_user")
                user_claims = configurable.get("user_claims", [])
                thread_id = configurable.get("thread_id", "anonymous_thread")
                active_action = configurable.get("active_action", "execute")

                # =========================================================================
                # 🛡️ STEP 1: INITIAL NODE ENTRY SECURITY (EXECUTE CHECK ONLY)
                # =========================================================================
                if active_action == "execute":
                    node_rules = full_policy.get(node_key, {})
                    required_claims_list = node_rules.get("execute", [])

                    if required_claims_list:
                        # 1. Block unauthenticated anonymous configurations completely
                        if "user_id" not in configurable or not user_claims:
                            raise PermissionError(
                                f"SECURITY INTERCEPTION: User '{user_id}' denied access to node '{node_key}'. "
                                f"Missing required permission claim tokens."
                            )

                        # 2. Enforce role claims clearance upfront before allowing node execution
                        is_authorized_entry = any(
                            claim in user_claims for claim in required_claims_list
                        )
                        if not is_authorized_entry:
                            raise PermissionError(
                                f"SECURITY INTERCEPTION: User '{user_id}' denied access to node '{node_key}'."
                            )

                # =========================================================================
                # 🔍 STEP 2: LOOKAHEAD & BACKGROUND ROUTING PASSES
                # =========================================================================
                ns_string = configurable.get("checkpoint_ns") or ""
                if not ns_string and "__pregel_runtime" in configurable:
                    runtime = configurable["__pregel_runtime"]
                    exec_info = getattr(runtime, "execution_info", None)
                    ns_string = getattr(exec_info, "checkpoint_ns", "") or ""

                if ns_string and ":" in ns_string:
                    active_executing_node = ns_string.split(":")[0]
                    if active_executing_node != node_key:
                        return _dispatch_runnable(orig_runnable, *args, **kwargs)

                # =========================================================================
                # 🛡️ STEP 3: RESUMPTION AUTHORIZATION (DYNAMIC LEDGER VERIFICATION)
                # =========================================================================
                if active_action == "resume":
                    from langgraph_gatekeeper.core.task_cache_db import (
                        get_token_by_business_context,
                    )

                    biz_ctx = configurable.get(
                        "active_business_context", "default_context"
                    )
                    token_data = get_token_by_business_context(thread_id, biz_ctx)

                    # FAIL-CLOSED PROTECTION: Hard-deny access on dynamic cache-miss anomalies
                    if not token_data:
                        raise PermissionError(
                            f"SECURITY INTERCEPTION: Resumption denied for user '{user_id}' on node '{node_key}'. "
                            f"No active transaction token found for business context '{biz_ctx}'."
                        )

                    required_claim = token_data.get("required_claim", "")
                    if required_claim and required_claim not in user_claims:
                        raise PermissionError(
                            f"SECURITY INTERCEPTION: User '{user_id}' denied resumption access to node '{node_key}'."
                        )

                return _dispatch_runnable(orig_runnable, *args, **kwargs)

            orig_name = getattr(
                getattr(original_runnable, "func", original_runnable),
                "__name__",
                node_key,
            )
            pre_execution_security_guard.__name__ = orig_name

            try:
                pre_execution_security_guard.__signature__ = inspect.signature(  # type: ignore
                    original_runnable
                )
            except (ValueError, TypeError):
                if hasattr(original_runnable, "invoke"):
                    pre_execution_security_guard.__signature__ = inspect.signature(  # type: ignore
                        original_runnable.invoke
                    )

            return pre_execution_security_guard

        secure_closure = create_secure_closure(
            node_name, original_runnable, policy_matrix
        )

        if hasattr(node_spec, "runnable"):
            node_spec.runnable = secure_closure
        elif hasattr(node_spec, "action"):
            node_spec.action = secure_closure
        else:
            workflow.nodes[node_name] = secure_closure

    compiled_graph = workflow.compile(**kwargs)
    return SecureCompiledGraph(compiled_graph, policy_matrix)
