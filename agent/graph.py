"""LangGraph state machine for the DQ triage agent.

Flow per breach:
  prompt → LLM (with tools bound) → decision
                                     ├── tool call → execute → back to LLM
                                     └── no tool call → END
"""
import operator
import os
from pathlib import Path
from typing import Annotated, Sequence, TypedDict
import httpx

# from langchain_anthropic import ChatAnthropic
from langchain_groq import ChatGroq
from langchain_core.messages import BaseMessage, SystemMessage
from langgraph.graph import END, StateGraph
from langgraph.prebuilt import ToolNode

from tools.kafka_tools import (
    quarantine_batch,
    propose_threshold_change,
    ignore_breach,
)
from tools.notify_tools import notify_human


# ---------------------------------------------------------------------------
# Tool catalog — agent can call any of these
# ---------------------------------------------------------------------------
TOOLS = [
    quarantine_batch,
    propose_threshold_change,
    ignore_breach,
    notify_human,
]


# ---------------------------------------------------------------------------
# LLM with tools bound
# ---------------------------------------------------------------------------
# MODEL_NAME = os.getenv("MODEL_NAME", "claude-sonnet-4-6")

# llm = ChatAnthropic(
#     model=MODEL_NAME,
#     max_tokens=2048,
#     temperature=0.2,           # low — we want consistent triage decisions
# ).bind_tools(TOOLS)

MODEL_NAME = os.getenv("MODEL_NAME", "llama-3.3-70b-versatile")

llm = ChatGroq(
    model=MODEL_NAME,
    max_tokens=2048,
    temperature=0.2,
    http_client=httpx.Client(verify=False),
).bind_tools(TOOLS)


# ---------------------------------------------------------------------------
# System prompt
# ---------------------------------------------------------------------------
PROMPT_PATH = Path(__file__).parent / "prompts" / "triage_system.txt"
SYSTEM_PROMPT = PROMPT_PATH.read_text(encoding="utf-8")


# ---------------------------------------------------------------------------
# Agent state — a running list of messages
# ---------------------------------------------------------------------------
class AgentState(TypedDict):
    messages: Annotated[Sequence[BaseMessage], operator.add]


# ---------------------------------------------------------------------------
# Node: LLM reasoning step
# ---------------------------------------------------------------------------
def reason(state: AgentState) -> dict:
    """Call the LLM once. It may decide to invoke a tool, or finalize."""
    messages = [SystemMessage(content=SYSTEM_PROMPT)] + list(state["messages"])
    response = llm.invoke(messages)
    return {"messages": [response]}


# ---------------------------------------------------------------------------
# Conditional edge: continue to tools, or done?
# ---------------------------------------------------------------------------
def should_continue(state: AgentState) -> str:
    """If last message has tool calls → execute tools. Otherwise stop."""
    last = state["messages"][-1]
    if getattr(last, "tool_calls", None):
        return "tools"
    return END


# ---------------------------------------------------------------------------
# Build the graph
# ---------------------------------------------------------------------------
workflow = StateGraph(AgentState)
workflow.add_node("reason", reason)
workflow.add_node("tools", ToolNode(TOOLS))

workflow.set_entry_point("reason")
workflow.add_conditional_edges("reason", should_continue)
workflow.add_edge("tools", "reason")    # after tool runs, LLM sees result

# Compile into runnable graph
graph = workflow.compile()