import inspect
from typing import Any, Callable, Dict, List

# Native framework configuration context primitive
from langgraph.config import get_config

from langgraph_gatekeeper.core.contracts import DefaultDictionaryPolicyProvider
from langgraph_gatekeeper.core.graph import SecureCompiledGraph


class SecureWorkflowGateway:
    """A centralized lifecycle security engine managing tool registration,
    node protection, and final graph compilation.
    """

    def __init__(self) -> None:
        self._policy_matrix: Dict[str, Any] = {}
        self._registered_tools: List[tuple] = []

    def add_tool(self, func: Callable, required_claim: str) -> "SecureWorkflowGateway":
        """Registers a raw python tool function with an explicit security permission token."""
        self._registered_tools.append((func, required_claim))
        return self

    def enforce_entry(
        self, node_name: str, required_claim: str
    ) -> "SecureWorkflowGateway":
        """Configures a static entry gate requirement directly on a graph canvas node."""
        self._policy_matrix[node_name] = {"required_claim": required_claim}
        return self

    @property
    def tools(self) -> Any:
        """Dynamically manufactures a native LangGraph ToolNode pre-wrapped
        with function-level, zero-trust security closures.
        """
        from langgraph.prebuilt import ToolNode

        secured_callables = []

        for func, required_claim in self._registered_tools:

            def create_secured_tool(target_fn=func, claim=required_claim) -> Callable:
                def secured_wrapper(*args: Any, **kwargs: Any) -> Any:
                    try:
                        config_dict = get_config() or {}
                    except Exception:
                        return target_fn(*args, **kwargs)

                    configurable = (
                        config_dict.get("configurable", {})
                        if isinstance(config_dict, dict)
                        else {}
                    )
                    user_claims = configurable.get("user_claims", [])
                    user_id = configurable.get("user_id", "anonymous_user")

                    # Option A: Enforce the exact string match at the tool boundary
                    if claim and claim not in user_claims:
                        raise PermissionError(
                            f"SECURITY INTERCEPTION: User '{user_id}' denied execution access to tool '{target_fn.__name__}'. "
                            f"Missing required permission claim '{claim}'."
                        )

                    return target_fn(*args, **kwargs)

                secured_wrapper.__name__ = target_fn.__name__
                secured_wrapper.__doc__ = target_fn.__doc__

                try:
                    secured_wrapper.__signature__ = inspect.signature(target_fn)
                except (ValueError, TypeError):
                    pass

                return secured_wrapper

            secured_callables.append(create_secured_tool())

        return ToolNode(secured_callables)

    def compile(self, workflow: Any, **kwargs: Any) -> SecureCompiledGraph:
        """Injects security closures across all nodes and compiles the final secure graph."""
        from langgraph_gatekeeper.core.task_cache_db import (
            get_token_by_business_context,
        )

        active_provider = DefaultDictionaryPolicyProvider(self._policy_matrix)

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

                    ns_string = configurable.get("checkpoint_ns") or ""
                    if not ns_string and "__pregel_runtime" in configurable:
                        runtime = configurable["__pregel_runtime"]
                        exec_info = getattr(runtime, "execution_info", None)
                        ns_string = getattr(exec_info, "checkpoint_ns", "") or ""

                    active_executing_node = (
                        ns_string.split(":")[0] if ns_string else None
                    )
                    if active_executing_node and active_executing_node != node_key:
                        return _dispatch_runnable(orig_runnable, *args, **kwargs)

                    if ns_string == "":
                        return _dispatch_runnable(orig_runnable, *args, **kwargs)

                    user_id = configurable.get("user_id", "anonymous_user")
                    user_claims = configurable.get("user_claims", [])
                    thread_id = configurable.get("thread_id", "anonymous_thread")
                    active_action = configurable.get("active_action", "execute")

                    if active_action == "resume":
                        biz_ctx = configurable.get(
                            "active_business_context", "default_context"
                        )
                        token_data = get_token_by_business_context(thread_id, biz_ctx)
                        required_claim = (
                            token_data.get("required_claim") if token_data else ""
                        )

                        if required_claim and required_claim not in user_claims:
                            raise PermissionError(
                                f"SECURITY INTERCEPTION: User '{user_id}' denied resumption access to node '{node_key}'. "
                                f"Missing required dynamic hurdle claim '{required_claim}'."
                            )
                    else:
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
        return SecureCompiledGraph(compiled_graph, self._policy_matrix)
