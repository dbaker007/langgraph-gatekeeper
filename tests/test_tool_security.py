import uuid

import pytest
from langchain_core.messages import AIMessage
from langgraph.checkpoint.memory import MemorySaver
from langgraph.graph import END, START, MessagesState, StateGraph

from langgraph_gatekeeper import SecureWorkflowGateway, execute_graph


def test_gateway_tool_management_clears_authorized_claims():
    """Verifies that a user holding the exact required tool permission claim
    can successfully execute a framework-wrapped tool function.
    """

    def mock_shipping_tool(cargo_id: str) -> dict:
        """Simulates shipping logistics operations tool footprint."""
        return {"status": "DELIVERED", "id": cargo_id}

    # 1. Register the tool with the gateway
    local_gw = SecureWorkflowGateway()
    local_gw.add_tool(mock_shipping_tool, required_claim="cargo:read")

    # 2. Build a standard graph topology using native MessagesState
    workflow = StateGraph(MessagesState)
    workflow.add_node("tools", local_gw.tools)
    workflow.add_edge(START, "tools")
    workflow.add_edge("tools", END)

    # 3. Compile the secure graph
    graph = local_gw.compile(workflow, checkpointer=MemorySaver())

    authorized_config = {
        "configurable": {
            "thread_id": f"t_tool_ok_{uuid.uuid4()}",
            "user_id": "operator_derek",
            "user_claims": ["cargo:read"],
        }
    }

    initial_state = {
        "messages": [
            AIMessage(
                content="",
                tool_calls=[
                    {
                        "name": "mock_shipping_tool",
                        "args": {"cargo_id": "CRG_999"},
                        "id": "call_id_123",
                        "type": "tool_call",
                    }
                ],
            )
        ]
    }

    # Execute the graph turn forward
    list(execute_graph(graph, initial_state, authorized_config))

    final_snapshot = graph.get_state(authorized_config)
    messages = final_snapshot.values.get("messages", [])

    assert len(messages) > 1
    assert "DELIVERED" in str(messages[-1].content)


def test_gateway_tool_management_blocks_unauthorized_claims():
    """Verifies that a user missing the required tool permission claim
    is forcefully blocked at the boundary with a hard PermissionError.
    """

    def mock_admin_payout_tool(amount: float) -> dict:
        """Enforces transactional escrow payout adjustments operations."""
        return {"escrow_status": "RELEASED", "payout": amount}

    local_gw = SecureWorkflowGateway()
    local_gw.add_tool(mock_admin_payout_tool, required_claim="financial:write")

    # Build standard graph topology using native MessagesState
    workflow = StateGraph(MessagesState)
    workflow.add_node("tools", local_gw.tools)
    workflow.add_edge(START, "tools")
    workflow.add_edge("tools", END)

    graph = local_gw.compile(workflow, checkpointer=MemorySaver())

    unauthorized_config = {
        "configurable": {
            "thread_id": f"t_tool_block_{uuid.uuid4()}",
            "user_id": "unverified_actor",
            "user_claims": ["cargo:read"],  # Lacks financial:write!
        }
    }

    initial_state = {
        "messages": [
            AIMessage(
                content="",
                tool_calls=[
                    {
                        "name": "mock_admin_payout_tool",
                        "args": {"amount": 5000.0},
                        "id": "call_id_456",
                        "type": "tool_call",
                    }
                ],
            )
        ]
    }

    # CORE EXCEPTION VERIFICATION: The framework MUST fail-closed and throw a hard PermissionError
    with pytest.raises(PermissionError) as exc_info:
        list(execute_graph(graph, initial_state, unauthorized_config))

    assert "denied execution access to tool 'mock_admin_payout_tool'" in str(
        exc_info.value
    )
    assert "Missing required permission claim 'financial:write'" in str(exc_info.value)
