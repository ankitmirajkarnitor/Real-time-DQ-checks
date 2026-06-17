"""Kafka-backed tools the agent invokes for routing/decisions."""
import json
import os
import uuid
from datetime import datetime, timezone

from kafka import KafkaProducer
from langchain_core.tools import tool


_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")

_producer = KafkaProducer(
    bootstrap_servers=_BOOTSTRAP,
    value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
    acks=1,
    linger_ms=10,
)


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


@tool
def quarantine_batch(batch_id: str, reason: str) -> str:
    """Mark an entire batch for full quarantine.
    Use ONLY for severe, batch-wide failures (e.g., 60%+ nulls,
    or critical schema breakdown). This is a strong action.

    Args:
        batch_id: The Spark batch_id identifier.
        reason: One-sentence explanation of why the whole batch is bad.
    """
    payload = {
        "action_id": str(uuid.uuid4()),
        "action":    "QUARANTINE_BATCH",
        "batch_id":  batch_id,
        "reason":    reason,
        "ts":        _now(),
    }
    _producer.send("agent_actions", value=payload)
    _producer.flush()
    return f"Batch {batch_id} marked for full quarantine. Reason: {reason}"


@tool
def propose_threshold_change(rule_id: str, current: float,
                              proposed: float, justification: str) -> str:
    """Propose adjusting a DQ rule's threshold.
    Use when sustained breaches at a similar level suggest the threshold
    is mis-calibrated, NOT when the data itself is bad.
    This action requires human approval before taking effect.

    Args:
        rule_id: e.g., 'R-COMPL-001'
        current: current threshold value
        proposed: new threshold value being suggested
        justification: why the change is warranted
    """
    payload = {
        "proposal_id": str(uuid.uuid4()),
        "type":        "THRESHOLD_CHANGE",
        "rule_id":     rule_id,
        "current":     current,
        "proposed":    proposed,
        "justification": justification,
        "status":      "AWAITING_APPROVAL",
        "ts":          _now(),
    }
    _producer.send("agent_proposals", value=payload)
    _producer.flush()
    return (f"Threshold change proposal recorded for {rule_id}: "
            f"{current} → {proposed} (awaiting approval).")


@tool
def ignore_breach(batch_id: str, rule_id: str, justification: str) -> str:
    """Acknowledge a breach but take no corrective action.
    Use ONLY when the breach is clearly transient, expected, or low-impact
    (e.g., one-off blip after dozens of clean batches).
    Always include strong reasoning - this is your most-audited action.

    Args:
        batch_id: The Spark batch_id identifier.
        rule_id: e.g., 'R-VALID-001'
        justification: why ignoring is the right call here
    """
    payload = {
        "action_id":  str(uuid.uuid4()),
        "action":     "IGNORE",
        "batch_id":   batch_id,
        "rule_id":    rule_id,
        "reason":     justification,
        "ts":         _now(),
    }
    _producer.send("agent_actions", value=payload)
    _producer.flush()
    return f"Breach ignored on batch={batch_id} rule={rule_id}: {justification}"