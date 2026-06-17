"""DQ triage agent — main loop.

Subscribes to observability_events. For every BREACH event, builds a prompt,
runs LangGraph, captures the decision and audit trail.

PASS events are recorded to history but no agent reasoning happens on them.
"""
import json
import logging
import os
import signal
import sys
import time
import uuid
from datetime import datetime, timezone

from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import KafkaError, NoBrokersAvailable
from langchain_core.messages import HumanMessage

from graph import graph
from state.store import StateStore


# ---------------------------------------------------------------------------
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
SOURCE_TOPIC    = os.getenv("OBS_TOPIC", "observability_events")
AUDIT_TOPIC     = os.getenv("AUDIT_TOPIC", "agent_audit")
GROUP_ID        = os.getenv("GROUP_ID", "dq-triage-agent")
DRY_RUN         = os.getenv("AGENT_DRY_RUN", "false").lower() == "true"


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("agent")


# ---------------------------------------------------------------------------
def build_prompt(event: dict, history: list[dict]) -> str:
    """Turn a breach event + history into an LLM prompt."""
    if history:
        hist_lines = []
        for row in history:
            ts = row.get("ts")
            ts_str = ts.strftime("%H:%M:%S") if hasattr(ts, "strftime") else str(ts)
            hist_lines.append(
                f"  {ts_str}  outcome={row.get('outcome'):<4}  "
                f"value={row.get('value')}"
            )
        history_text = "\n".join(hist_lines)
    else:
        history_text = "  (no prior history for this rule)"

    return f"""A DQ breach has occurred.

Batch ID:        {event.get('batch_id')}
Rule:            {event.get('rule_id')}
Dimension:       {event.get('dq_dimension')}
Severity:        {event.get('severity')}
Measured value:  {event.get('value')}
Threshold:       {event.get('threshold')}
Status:          {event.get('status')}
Timestamp:       {event.get('timestamp')}

Recent history for this rule (newest first):
{history_text}

Decide on exactly ONE action. Justify your choice briefly.
"""


# ---------------------------------------------------------------------------
def extract_tool_summary(messages) -> str:
    """Walk the message history, return a compact string of tool calls made."""
    parts = []
    for m in messages:
        tcs = getattr(m, "tool_calls", None) or []
        for tc in tcs:
            args = tc.get("args", {})
            parts.append(f"{tc.get('name')}({json.dumps(args, default=str)[:200]})")
    return " ; ".join(parts) if parts else "NO_TOOL_CALLED"


def extract_reasoning(messages) -> str:
    """Pull text content from the final assistant message."""
    if not messages:
        return ""
    last = messages[-1]
    content = getattr(last, "content", "")
    if isinstance(content, list):
        # Anthropic can return list of content blocks
        return " ".join(
            block.get("text", "") if isinstance(block, dict) else str(block)
            for block in content
        )
    return str(content)


# ---------------------------------------------------------------------------
def connect_consumer():
    """Connect to Kafka with retry."""
    while True:
        try:
            c = KafkaConsumer(
                SOURCE_TOPIC,
                bootstrap_servers=KAFKA_BOOTSTRAP,
                group_id=GROUP_ID,
                auto_offset_reset="latest",
                enable_auto_commit=True,
                value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            )
            log.info("consumer connected to %s topic=%s",
                     KAFKA_BOOTSTRAP, SOURCE_TOPIC)
            return c
        except (KafkaError, NoBrokersAvailable) as e:
            log.warning("Kafka not ready (%s) — retry in 5s", e)
            time.sleep(5)


# ---------------------------------------------------------------------------
def main():
    log.info("=== DQ triage agent starting ===")
    log.info("dry_run=%s model=%s", DRY_RUN, os.getenv("MODEL_NAME", "claude-sonnet-4-6"))

    store = StateStore()
    log.info("state store connected")

    audit_producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
        acks=1,
        linger_ms=10,
    )

    consumer = connect_consumer()

    # Graceful shutdown
    stop = {"flag": False}
    signal.signal(signal.SIGINT,  lambda *_: stop.update(flag=True))
    signal.signal(signal.SIGTERM, lambda *_: stop.update(flag=True))

    for msg in consumer:
        if stop["flag"]:
            break

        try:
            event = msg.value
            batch_id = str(event.get("batch_id"))
            rule_id  = event.get("rule_id", "UNKNOWN")
            status   = event.get("status", "UNKNOWN")

            # 1. Always record into history (pass or fail)
            store.record_check(batch_id, rule_id, status, event.get("value"))

            # 2. Only reason on BREACH events
            if status != "BREACH":
                continue

            log.info("breach: batch=%s rule=%s severity=%s value=%s",
                     batch_id, rule_id, event.get("severity"), event.get("value"))

            history = store.recent_history(rule_id, n=10)
            prompt = build_prompt(event, history)

            decision_id = str(uuid.uuid4())

            if DRY_RUN:
                log.info("DRY_RUN: would invoke LLM on decision_id=%s", decision_id)
                continue

            # 3. Invoke the agent
            try:
                result = graph.invoke({"messages": [HumanMessage(content=prompt)]})
                messages = result.get("messages", [])

                tool_summary = extract_tool_summary(messages)
                reasoning    = extract_reasoning(messages)

                log.info("decision %s: tools=%s", decision_id, tool_summary)

                store.record_decision(
                    decision_id=decision_id,
                    batch_id=batch_id,
                    rule_id=rule_id,
                    tool=tool_summary,
                    reasoning=reasoning,
                    outcome="DONE",
                )

                # 4. Full audit trail goes to Kafka
                audit_producer.send(AUDIT_TOPIC, value={
                    "decision_id":    decision_id,
                    "trigger_event":  event,
                    "tool_calls":     tool_summary,
                    "reasoning":      reasoning,
                    "ts":             datetime.now(timezone.utc).isoformat(),
                })

            except Exception as e:
                log.exception("LLM invocation failed for decision_id=%s", decision_id)
                store.record_decision(
                    decision_id=decision_id,
                    batch_id=batch_id,
                    rule_id=rule_id,
                    tool="ERROR",
                    reasoning=f"Agent crashed: {e}",
                    outcome="ERROR",
                )
                audit_producer.send(AUDIT_TOPIC, value={
                    "decision_id":   decision_id,
                    "trigger_event": event,
                    "error":         str(e),
                    "ts":            datetime.now(timezone.utc).isoformat(),
                })

        except Exception as e:
            log.exception("event processing failed: %s", e)

    log.info("shutdown")
    consumer.close()
    audit_producer.close()
    store.close()


if __name__ == "__main__":
    main()