import inspect
from typing import Any, Callable, Dict, List

from langgraph.config import get_config

from langgraph_gatekeeper.core.contracts import DefaultDictionaryPolicyProvider
from langgraph_gatekeeper.core.graph import SecureCompiledGraph

# =============================================================================
# GLOBAL FRAMEWORK TOPOLOGY CONSTANTS
# =============================================================================
# Framework consumers MUST register the secure tools node on their StateGraph
# using this exact literal string key name. Custom alternative keys will collide
# with LangGraph's native parameter dependency injector.
SECURE_TOOL_NODE_NAME = "tools"


class SecureWorkflowGateway:
    """A centralized lifecycle security engine managing tool registration,
    node protection, and final graph compilation.
    """

    def __init__(self) -> None:
        self._policy_matrix: Dict[str, Any] = {}
        self._registered_tools: Dict[Callable, str] = {}
        self._tool_claims_map: Dict[str, str] = {}

    def add_tool(self, func: Callable, required_claim: str) -> "SecureWorkflowGateway":
        """Registers a raw python tool function with an explicit security permission token.

        CRITICAL ARCHITECTURAL REQUIREMENT:
        When using secure tools, you MUST mount the framework-managed tools node onto
        your StateGraph using the canonical name 'tools' (or SECURE_TOOL_NODE_NAME):

            workflow.add_node(SECURE_TOOL_NODE_NAME, gateway.tools)

        Naming this node anything else will cause LangGraph's internal dependency injector
        to strip method signature schemas and fail with a native ValueError at runtime.
        """
        self._tool_claims_map[func.__name__] = required_claim
        self._registered_tools[func] = required_claim
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
        with node-level, zero-trust security closures.
        """
        from langgraph.prebuilt import ToolNode

        native_tool_node = ToolNode(list(self._registered_tools.keys()))
        claims_map = self._tool_claims_map

        def secure_tool_node_executor(state: Any, config: Any = None) -> Any:
            config_dict = {}
            if config:
                config_dict = (
                    config
                    if isinstance(config, dict)
                    else getattr(config, "config", getattr(config, "__dict__", {}))
                )
            else:
                try:
                    config_dict = get_config() or {}
                except Exception:
                    pass

            if not isinstance(config_dict, dict):
                config_dict = getattr(
                    config_dict, "config", getattr(config_dict, "__dict__", {})
                )

            configurable = (
                config_dict.get("configurable", {})
                if isinstance(config_dict, dict)
                else {}
            )
            user_claims = configurable.get("user_claims", [])
            user_id = configurable.get("user_id", "anonymous_user")

            messages = []
            if isinstance(state, dict):
                messages = state.get("messages", [])
            elif hasattr(state, "messages"):
                messages = state.messages
            elif isinstance(state, list):
                messages = state

            if messages:
                last_msg = messages[-1] if isinstance(messages, list) else messages
                tool_calls = getattr(last_msg, "tool_calls", None) or (
                    last_msg.get("tool_calls") if isinstance(last_msg, dict) else None
                )

                if tool_calls:
                    for call in tool_calls:
                        call_dict = (
                            call
                            if isinstance(call, dict)
                            else getattr(call, "model_dump", lambda: {})()
                        )
                        tool_name = call_dict.get("name") or call_dict.get(
                            "function", {}
                        ).get("name")

                        required_claim = claims_map.get(tool_name)
                        if required_claim and required_claim not in user_claims:
                            raise PermissionError(
                                f"SECURITY INTERCEPTION: User '{user_id}' denied execution access to tool '{tool_name}'. "
                                f"Missing required permission claim '{required_claim}'."
                            )

            return native_tool_node.invoke(state, config=config)

        secure_tool_node_executor.__name__ = SECURE_TOOL_NODE_NAME
        return secure_tool_node_executor

    def compile(self, workflow: Any, **kwargs: Any) -> SecureCompiledGraph:
        """Injects security closures across all nodes and compiles the final secure graph."""
        from langgraph_gatekeeper.core.task_cache_db import (
            get_token_by_business_context,
        )

        # 1. Enforce message-tracking duck typing conditionally if tools are registered
        if self._registered_tools:
            has_message_channel = "messages" in workflow.channels
            schema_obj = getattr(workflow, "schema", None)

            has_message_field = False
            if schema_obj:
                if hasattr(schema_obj, "__fields__"):  # Pydantic v1
                    has_message_field = "messages" in schema_obj.__fields__
                elif hasattr(schema_obj, "model_fields"):  # Pydantic v2
                    has_message_field = "messages" in schema_obj.model_fields
                elif hasattr(
                    schema_obj, "__annotations__"
                ):  # TypedDict / Standard Class
                    has_message_field = "messages" in schema_obj.__annotations__

            if not (has_message_channel or has_message_field):
                raise TypeError(
                    "\n================================================================================\n"
                    "CRITICAL COMPILATION REFUSAL: Missing Mandatory 'messages' State Channel\n"
                    "================================================================================\n"
                    "The provided StateGraph schema does not support message-tracking capabilities.\n\n"
                    "WHY THIS CRITICAL ERROR OCCURRED:\n"
                    "You have registered secure tools using '.add_tool()'. The framework compiles a native\n"
                    "LangGraph ToolNode under the hood, which strictly requires an explicit state key\n"
                    "named exactly 'messages' to extract, process, and write back LLM tool call loops.\n\n"
                    "HOW TO FIX THIS REJECTION:\n"
                    "1. Update your StateGraph schema class or TypedDict to include a key named 'messages'.\n"
                    "2. Alternatively, inherit directly from LangGraph's pre-built 'MessagesState' primitive:\n\n"
                    "   from langgraph.graph import MessagesState, StateGraph\n"
                    "   workflow = StateGraph(MessagesState)\n"
                    "================================================================================"
                )

        active_provider = DefaultDictionaryPolicyProvider(self._policy_matrix)

        def _dispatch_runnable(runnable_obj: Any, *a: Any, **kw: Any) -> Any:
            if hasattr(runnable_obj, "invoke") and callable(runnable_obj.invoke):
                return runnable_obj.invoke(*a, **kw)
            return runnable_obj(*a, **kw)

        # 2. Safety scan to protect the developer from naming convention collisions
        canvas_node_keys = list(workflow.nodes.keys())
        if self._registered_tools and SECURE_TOOL_NODE_NAME not in canvas_node_keys:
            raise KeyError(
                f"\n================================================================================\n"
                f"CRITICAL TOPOLOGY REJECTION: Secure Tools Node Key Mismatch\n"
                f"================================================================================\n"
                f"You have registered secure tools, but the framework cannot locate the mandatory\n"
                f"'{SECURE_TOOL_NODE_NAME}' node on your StateGraph canvas.\n\n"
                f"To utilize tool-level capability firewalls, you MUST register the framework's tool\n"
                f"node using the explicit string key constant 'SECURE_TOOL_NODE_NAME':\n\n"
                f"   workflow.add_node(SECURE_TOOL_NODE_NAME, gateway.tools)\n"
                f"================================================================================"
            )

        for node_name in canvas_node_keys:
            if node_name == SECURE_TOOL_NODE_NAME:
                continue

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

                    # FIXED: Added [0] to cleanly map the target node name string from the namespace list
                    if ns_string and ":" in ns_string:
                        active_executing_node = ns_string.split(":")[0]
                        if active_executing_node != node_key:
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
