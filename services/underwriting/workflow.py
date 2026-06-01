import uuid
from typing import Any, Optional

from langgraph.checkpoint.base import RunnableConfig
from langgraph.checkpoint.memory import MemorySaver
from langgraph.graph import END, START, StateGraph
from pydantic import BaseModel, Field

# CORRECTED: Pull from flat core package facade
from core import compile_graph_with_authorization, interrupt


class ProjectState(BaseModel):
    customer_state: str = "OH"
    customer_name: str = "Nathan"
    household_income: int = 120000
    application_id: str = "app_974350"
    routing_key: str = ""
    result_data: str = ""
    kill_switch_reason: str = ""
    agent_notes: list = Field(default_factory=list)


def post_request_to_task_management_system(
    routing_key: str, node_name: str, state: ProjectState
) -> None:
    pass


def assign_agent(state: ProjectState, config: Optional[RunnableConfig] = None) -> dict:
    unique_key = f"task_concierge_{uuid.uuid4()}"
    post_request_to_task_management_system(unique_key, "assign_agent", state)
    response = interrupt({"routing_key": unique_key})
    return {"routing_key": unique_key, "result_data": response}


def assign_credit(state: ProjectState, config: Optional[RunnableConfig] = None) -> dict:
    unique_key = f"task_credit_{uuid.uuid4()}"
    post_request_to_task_management_system(unique_key, "assign_credit", state)
    response = interrupt({"routing_key": unique_key})
    return {"routing_key": unique_key, "result_data": response}


def kill_switch(state: ProjectState, config: Optional[RunnableConfig] = None) -> dict:
    return {}


def eviction_cleanup(
    state: ProjectState, config: Optional[RunnableConfig] = None
) -> dict:
    return {
        "agent_notes": [
            f"Platform eviction executed. Reason: {state.kill_switch_reason}"
        ]
    }


workflow = StateGraph(ProjectState)
workflow.add_node("assign_agent", assign_agent)
workflow.add_node("assign_credit", assign_credit)
workflow.add_node("kill_switch", kill_switch)
workflow.add_node("eviction_cleanup", eviction_cleanup)

workflow.add_edge(START, "assign_agent")
workflow.add_edge("assign_agent", "assign_credit")
workflow.add_edge("assign_credit", END)
workflow.add_edge("kill_switch", "eviction_cleanup")
workflow.add_edge("eviction_cleanup", END)

checkpointer = MemorySaver()
graph = compile_graph_with_authorization(workflow, checkpointer=checkpointer)
