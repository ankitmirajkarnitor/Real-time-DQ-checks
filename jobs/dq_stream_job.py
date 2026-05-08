"""
dq_stream_job.py — Phase 2 (v2): full DQ pipeline.

Flow per micro-batch (every 30 seconds):
  orders_raw  ──►  parse JSON
              ──►  record-level validation (PySpark):
                       GOOD records  ──►  orders_clean
                       BAD  records  ──►  orders_quarantine (with reason)
              ──►  batch-level DQ (Soda Core):
                       summary metrics  ──►  observability_events

Key fix vs v1:
  - Uses batch_df.sparkSession (the session the DataFrame is bound to)
    instead of the captured outer `spark`. This resolves the
    TABLE_OR_VIEW_NOT_FOUND error for `orders_batch`.
"""
from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType
)

# ---------------------------------------------------------------------------
KAFKA_BOOTSTRAP   = "kafka:9092"
SOURCE_TOPIC      = "orders_raw"
CLEAN_TOPIC       = "orders_clean"
QUARANTINE_TOPIC  = "orders_quarantine"
OBS_TOPIC         = "observability_events"
TRIGGER_SEC       = 30
CHECKPOINT_BASE   = "/opt/checkpoints/dq_stream_job"

# Top of file
RULE_SEVERITY = {
    "R-COMPL-001": "WARN",
    "R-COMPL-002": "CRITICAL",
    "R-UNIQ-001":  "CRITICAL",
    "R-VALID-001": "WARN",
    "R-VALID-002": "WARN",
    "R-VOL-001":   "CRITICAL",
}
RULE_DIMENSION = {
    "R-COMPL-001": "completeness",
    "R-COMPL-002": "completeness",
    "R-UNIQ-001":  "uniqueness",
    "R-VALID-001": "validity",
    "R-VALID-002": "validity",
    "R-VOL-001":   "volume",
}

# Map check name → rule_id (since Soda's attributes may not pass through reliably)
RULE_NAME_TO_ID = {
    "Completeness – customer_id null percentage under 5%": "R-COMPL-001",
    "Completeness – order_id never null":                  "R-COMPL-002",
    "Uniqueness – order_id unique within window":          "R-UNIQ-001",
    "Validity – email format compliance":                  "R-VALID-001",
    "Validity – quantity must be positive integer":        "R-VALID-002",
    "Volume – batch is non-empty":                         "R-VOL-001",
}

ORDER_SCHEMA = StructType([
    StructField("order_id",      StringType()),
    StructField("customer_id",   StringType()),
    StructField("product_id",    StringType()),
    StructField("quantity",      IntegerType()),
    StructField("amount",        DoubleType()),
    StructField("currency",      StringType()),
    StructField("email",         StringType()),
    StructField("status",        StringType()),
    StructField("event_time",    StringType()),
    StructField("source_region", StringType()),
])

EMAIL_RE = r"^[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}$"


# ---------------------------------------------------------------------------
# def to_obs_event(check: dict, batch_id: int, batch_size: int) -> dict:
#     attrs  = (check.get("attributes") or {}) or {}
#     status = "PASS" if check.get("outcome") == "pass" else "BREACH"
#     sev    = attrs.get("severity", "warn").upper() if status == "BREACH" else "INFO"
#     return {
#         "event_id":     str(uuid.uuid4()),
#         "pipeline_id":  "orders-ingestion-pipeline",
#         "batch_id":     str(batch_id),
#         "entity":       f"kafka.{SOURCE_TOPIC}",
#         "metric":       check.get("name"),
#         "dq_dimension": attrs.get("dimension"),
#         "rule_id":      attrs.get("rule_id"),
#         "value":        check.get("value"),
#         "threshold":    check.get("threshold"),
#         "status":       status,
#         "severity":     sev,
#         "timestamp":    datetime.now(timezone.utc).isoformat(),
#         "metadata": {
#             "batch_size":    batch_size,
#             "check_outcome": check.get("outcome"),
#         },
#     }

def resolve_rule_id(name: str) -> str:
    name_lower = (name or "").lower()
    if "customer_id null percentage" in name_lower: return "R-COMPL-001"
    if "order_id never null"          in name_lower: return "R-COMPL-002"
    if "order_id unique"              in name_lower: return "R-UNIQ-001"
    if "email format"                 in name_lower: return "R-VALID-001"
    if "quantity must be positive"    in name_lower: return "R-VALID-002"
    if "batch is non-empty"           in name_lower: return "R-VOL-001"
    return "UNKNOWN"

def _extract_diag_value(diagnostics: dict):
    if not isinstance(diagnostics, dict):
        return None, None
    val = diagnostics.get("value")
    if not isinstance(val, (int, float)):
        val = None

    fail = diagnostics.get("fail") or {}
    threshold = (fail.get("greaterThan")
                 or fail.get("greaterThanOrEqual")
                 or fail.get("lessThan")
                 or fail.get("lessThanOrEqual"))
    # 0.0 is falsy in Python's `or` — fix that
    for key in ("greaterThan", "greaterThanOrEqual", "lessThan", "lessThanOrEqual"):
        if key in fail:
            threshold = fail[key]
            break
    if not isinstance(threshold, (int, float)):
        threshold = None
    return val, threshold


def _resource_attrs(check: dict) -> dict:
    """Soda exposes our YAML attributes here (not under 'attributes')."""
    return {a["name"]: a["value"]
            for a in (check.get("resourceAttributes") or [])
            if isinstance(a, dict) and "name" in a}


def to_obs_event(check: dict, batch_id: int, batch_size: int) -> dict:
    name = check.get("name", "")
    attrs = _resource_attrs(check)

    # Prefer Soda-provided attributes; fall back to substring match if missing
    rule_id   = attrs.get("rule_id")   or resolve_rule_id(name)
    dimension = attrs.get("dimension") or RULE_DIMENSION.get(rule_id, "unknown")
    yaml_sev  = (attrs.get("severity") or "warn").upper()

    status   = "PASS" if check.get("outcome") == "pass" else "BREACH"
    severity = yaml_sev if status == "BREACH" else "INFO"

    value, threshold = _extract_diag_value(check.get("diagnostics") or {})

    return {
        "event_id":     str(uuid.uuid4()),
        "pipeline_id":  "orders-ingestion-pipeline",
        "batch_id":     str(batch_id),
        "entity":       f"kafka.{SOURCE_TOPIC}",
        "metric":       name,
        "dq_dimension": dimension,
        "rule_id":      rule_id,
        "value":        value,
        "threshold":    threshold,
        "status":       status,
        "severity":     severity,
        "timestamp":    datetime.now(timezone.utc).isoformat(),
        "metadata": {
            "batch_size":    batch_size,
            "check_outcome": check.get("outcome"),
        },
    }

# def _safe_number(v):
#     if v is None:
#         return None
#     if isinstance(v, (int, float)):
#         return float(v)
#     if isinstance(v, str):
#         try:
#             return float(v.rstrip("%").strip())
#         except ValueError:
#             return None
#     return None

# def to_obs_event(check: dict, batch_id: int, batch_size: int) -> dict:
#     name = check.get("name", "")
#     rule_id = resolve_rule_id(name)
#     dimension = RULE_DIMENSION.get(rule_id, "unknown")

#     status = "PASS" if check.get("outcome") == "pass" else "BREACH"
#     severity = RULE_SEVERITY.get(rule_id, "WARN") if status == "BREACH" else "INFO"

#     # Soda's metric value can live in different places across versions
#     value = (_safe_number(check.get("value"))
#              or _safe_number(check.get("metric_value")))

#     # Last resort: try metrics[0]["value"] only if it's actually a dict
#     if value is None:
#         metrics = check.get("metrics") or []
#         if metrics and isinstance(metrics[0], dict):
#             value = _safe_number(metrics[0].get("value"))

#     # Threshold: parse from check definition string if not directly provided
#     threshold = _safe_number(check.get("threshold"))
#     if threshold is None:
#         # parse from check name like "missing_percent(customer_id) < 1"
#         import re
#         defn = check.get("definition", "") or check.get("expression", "")
#         m = re.search(r"[<>=]+\s*([\d.]+)", defn)
#         if m:
#             threshold = float(m.group(1))

#     return {
#         "event_id":     str(uuid.uuid4()),
#         "pipeline_id":  "orders-ingestion-pipeline",
#         "batch_id":     str(batch_id),
#         "entity":       f"kafka.{SOURCE_TOPIC}",
#         "metric":       name,
#         "dq_dimension": dimension,
#         "rule_id":      rule_id,
#         "value":        value,
#         "threshold":    threshold,
#         "status":       status,
#         "severity":     severity,
#         "timestamp":    datetime.now(timezone.utc).isoformat(),
#         "metadata": {
#             "batch_size":    batch_size,
#             "check_outcome": check.get("outcome"),
#         },
#     }


# def to_obs_event(check: dict, batch_id: int, batch_size: int) -> dict:
#     name = check.get("name", "")
#     # rule_id = RULE_NAME_TO_ID.get(name, "UNKNOWN")
#     rule_id = resolve_rule_id(check.get("name"))
#     dimension = RULE_DIMENSION.get(rule_id, "unknown")

#     status = "PASS" if check.get("outcome") == "pass" else "BREACH"
#     severity = RULE_SEVERITY.get(rule_id, "WARN") if status == "BREACH" else "INFO"

#     return {
#         "event_id":     str(uuid.uuid4()),
#         "pipeline_id":  "orders-ingestion-pipeline",
#         "batch_id":     str(batch_id),
#         "entity":       f"kafka.{SOURCE_TOPIC}",
#         "metric":       name,
#         "dq_dimension": dimension,
#         "rule_id":      rule_id,
#         "value":        check.get("value"),
#         "threshold":    check.get("threshold"),
#         "status":       status,
#         "severity":     severity,
#         "timestamp":    datetime.now(timezone.utc).isoformat(),
#         "metadata": {
#             "batch_size":    batch_size,
#             "check_outcome": check.get("outcome"),
#         },
#     }


# ---------------------------------------------------------------------------
def build_handler():
    def handler(batch_df: DataFrame, batch_id: int):
        # CRITICAL FIX: use the session the DataFrame belongs to,
        # not the outer `spark` variable.
        spark = batch_df.sparkSession

        batch_df = batch_df.persist()
        size = batch_df.count()
        if size == 0:
            batch_df.unpersist()
            return

        # --- 1. Record-level validation: branch to clean vs quarantine ------
        bad_cond = (
            F.col("order_id").isNull() |
            F.col("customer_id").isNull() |
            F.col("quantity").isNull() |
            (F.col("quantity") < 1) | (F.col("quantity") > 1000) |
            (F.col("email").isNotNull() & ~F.col("email").rlike(EMAIL_RE))
        )

        reason = (
            F.when(F.col("order_id").isNull(),     "NULL_ORDER_ID")
             .when(F.col("customer_id").isNull(),  "NULL_CUSTOMER_ID")
             .when(F.col("quantity").isNull(),     "NULL_QUANTITY")
             .when((F.col("quantity") < 1) | (F.col("quantity") > 1000), "INVALID_QUANTITY_RANGE")
             .when(F.col("email").isNotNull() & ~F.col("email").rlike(EMAIL_RE), "INVALID_EMAIL")
             .otherwise("OK")
        )

        annotated = (batch_df
                     .withColumn("_is_bad", bad_cond)
                     .withColumn("_reason", reason))

        clean_df      = annotated.filter(~F.col("_is_bad")).drop("_is_bad", "_reason")
        quarantine_df = annotated.filter( F.col("_is_bad"))

        n_clean      = clean_df.count()
        n_quarantine = quarantine_df.count()

        # Write CLEAN records → orders_clean
        if n_clean > 0:
            (clean_df
                .select(F.to_json(F.struct(*clean_df.columns)).alias("value"))
                .write
                .format("kafka")
                .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
                .option("topic", CLEAN_TOPIC)
                .save())

        # Write BAD records → orders_quarantine (with reason + batch id)
        if n_quarantine > 0:
            quar_payload = (quarantine_df
                            .withColumn("_batch_id", F.lit(batch_id))
                            .withColumn("_quarantined_at", F.current_timestamp().cast("string")))
            (quar_payload
                .select(F.to_json(F.struct(*quar_payload.columns)).alias("value"))
                .write
                .format("kafka")
                .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
                .option("topic", QUARANTINE_TOPIC)
                .save())

        # --- 2. Batch-level DQ: Soda Core -----------------------------------
        batch_df.createOrReplaceTempView("orders_batch")

        from soda.scan import Scan
        scan = Scan()
        scan.set_scan_definition_name(f"orders_batch_{batch_id}")
        scan.set_data_source_name("spark_df")
        scan.add_spark_session(spark, data_source_name="spark_df")
        scan.add_sodacl_yaml_file("/opt/jobs/dq/orders_checks.yml")
        scan.execute()

        results = scan.get_scan_results() or {}
        checks  = results.get("checks", [])
        # for c in checks:
        #     print(f"[soda] {c.get('name'):<60} outcome={c.get('outcome')} "
        #         f"value={c.get('value')} threshold={c.get('threshold')}")
        # for c in checks:
        #     print(f"[soda-name] {repr(c.get('name'))}")
        for c in checks:
            print(f"[soda-full] === {c.get('name')} ===")
            print(json.dumps(c, indent=2, default=str))

        events  = [to_obs_event(c, batch_id, size) for c in checks]

        if events:
            obs_df = spark.createDataFrame(
                [(json.dumps(e),) for e in events], ["value"]
            )
            (obs_df.write
                .format("kafka")
                .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
                .option("topic", OBS_TOPIC)
                .save())

        n_breach = sum(1 for e in events if e["status"] == "BREACH")
        print(f"[dq] batch={batch_id} size={size} "
              f"clean={n_clean} quarantined={n_quarantine} "
              f"checks={len(events)} breaches={n_breach}")

        batch_df.unpersist()

    return handler


# ---------------------------------------------------------------------------
def main():
    spark = (SparkSession.builder
             .appName("dq_stream_job")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    raw = (spark.readStream
           .format("kafka")
           .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
           .option("subscribe", SOURCE_TOPIC)
           .option("startingOffsets", "latest")
           .load())

    parsed = (raw
              .selectExpr("CAST(value AS STRING) AS json_str", "timestamp AS ingest_ts")
              .select(F.from_json("json_str", ORDER_SCHEMA).alias("o"),
                      F.col("ingest_ts"))
              .select("o.*", "ingest_ts"))

    query = (parsed.writeStream
             .foreachBatch(build_handler())
             .outputMode("append")
             .trigger(processingTime=f"{TRIGGER_SEC} seconds")
             .option("checkpointLocation", CHECKPOINT_BASE)
             .start())

    query.awaitTermination()


if __name__ == "__main__":
    main()
