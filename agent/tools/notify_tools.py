"""Notification tools - send alerts to humans via Slack and audit log."""
import json
import os
import uuid
from datetime import datetime, timezone

from kafka import KafkaProducer
from langchain_core.tools import tool


_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
_SLACK_URL = os.getenv("SLACK_WEBHOOK_URL", "").strip()

_producer = KafkaProducer(
    bootstrap_servers=_BOOTSTRAP,
    value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
    acks=1,
    linger_ms=10,
)


# Lazy import Slack so it works even if not configured
try:
    from slack_sdk.webhook import WebhookClient
    _slack = WebhookClient(_SLACK_URL) if _SLACK_URL else None
except Exception:
    _slack = None


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


@tool
def notify_human(severity: str, summary: str, evidence: dict) -> str:
    """Escalate the breach to the on-call engineer.
    Use when the breach is severe, novel, or you are genuinely uncertain
    which action is correct.

    Args:
        severity: 'WARN' or 'CRITICAL'
        summary: one-line description of the issue
        evidence: dict with batch_id, rule_id, value, threshold, history
    """
    # Always record an action event for audit
    payload = {
        "action_id": str(uuid.uuid4()),
        "action":    "NOTIFY_HUMAN",
        "severity":  severity,
        "summary":   summary,
        "evidence":  evidence,
        "ts":        _now(),
    }
    _producer.send("agent_actions", value=payload)
    _producer.flush()

    # Send Slack message if configured
    if _slack is not None:
        try:
            _slack.send(text=(
                f"*[DQ {severity}]* {summary}\n"
                f"```{json.dumps(evidence, indent=2, default=str)}```"
            ))
        except Exception as e:
            return f"Notified (audit only — Slack failed: {e})"
        return f"Notified on-call via Slack: {summary}"

    return f"Notification recorded (Slack not configured): {summary}"