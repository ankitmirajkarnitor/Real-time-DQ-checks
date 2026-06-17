"""Postgres-backed state store for the DQ agent."""
import os
import psycopg2
from psycopg2.extras import RealDictCursor


class StateStore:
    def __init__(self, dsn: str | None = None):
        self.dsn = dsn or os.getenv(
            "POSTGRES_DSN",
            "postgresql://dq:dq@postgres:5432/dq",
        )
        self.conn = psycopg2.connect(self.dsn)
        self.conn.autocommit = True
        self._init_schema()

    def _init_schema(self):
        with self.conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS batch_history (
                    id         SERIAL PRIMARY KEY,
                    batch_id   TEXT NOT NULL,
                    rule_id    TEXT NOT NULL,
                    outcome    TEXT NOT NULL,
                    value      DOUBLE PRECISION,
                    ts         TIMESTAMPTZ DEFAULT now()
                );
                CREATE INDEX IF NOT EXISTS idx_history_rule_ts
                    ON batch_history(rule_id, ts DESC);

                CREATE TABLE IF NOT EXISTS agent_decisions (
                    decision_id  TEXT PRIMARY KEY,
                    batch_id     TEXT NOT NULL,
                    rule_id      TEXT NOT NULL,
                    tool         TEXT,
                    reasoning    TEXT,
                    outcome      TEXT,
                    ts           TIMESTAMPTZ DEFAULT now()
                );
            """)

    def record_check(self, batch_id: str, rule_id: str,
                     outcome: str, value):
        """Save every check result (pass or fail) for history."""
        try:
            v = float(value) if value is not None else None
        except (TypeError, ValueError):
            v = None
        with self.conn.cursor() as cur:
            cur.execute(
                "INSERT INTO batch_history(batch_id, rule_id, outcome, value) "
                "VALUES (%s, %s, %s, %s)",
                (batch_id, rule_id, outcome, v),
            )

    def recent_history(self, rule_id: str, n: int = 10) -> list[dict]:
        """Last N outcomes for a rule, newest first."""
        with self.conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                "SELECT batch_id, ts, outcome, value FROM batch_history "
                "WHERE rule_id = %s ORDER BY ts DESC LIMIT %s",
                (rule_id, n),
            )
            return [dict(row) for row in cur.fetchall()]

    def record_decision(self, decision_id: str, batch_id: str,
                        rule_id: str, tool: str, reasoning: str,
                        outcome: str = "DONE"):
        with self.conn.cursor() as cur:
            cur.execute(
                "INSERT INTO agent_decisions "
                "(decision_id, batch_id, rule_id, tool, reasoning, outcome) "
                "VALUES (%s, %s, %s, %s, %s, %s) "
                "ON CONFLICT (decision_id) DO NOTHING",
                (decision_id, batch_id, rule_id, tool, reasoning, outcome),
            )

    def close(self):
        if self.conn:
            self.conn.close()
