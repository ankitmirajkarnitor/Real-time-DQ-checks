"""
dq_stream_job.py — Phase A: Great Expectations engine.

Flow per micro-batch:
  orders_raw  ──►  parse JSON
              ──►  record-level routing (PySpark):
                       GOOD  ──►  orders_clean
                       BAD   ──►  orders_quarantine
              ──►  batch-level DQ (Great Expectations):
                       results ──►  observability_events
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

import great_expectations as gx

# ---------------------------------------------------------------------------
KAFKA_BOOTSTRAP   = "kafka:9092"
SOURCE_TOPIC      = "orders_raw"
CLEAN_TOPIC       = "orders_clean"
QUARANTINE_TOPIC  = "orders_quarantine"
OBS_TOPIC         = "observability_events"
TRIGGER_SEC       = 30
CHECKPOINT_BASE   = "/opt/checkpoints/dq_stream_job"
SUITE_PATH        = "/opt/jobs/dq/orders_suite.json"

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

# Load GE suite once at module level
with open(SUITE_PATH) as f:
    SUITE_DICT = json.load(f)


# ---------------------------------------------------------------------------
def _extract_value(result_dict: dict):
    """Pull a meaningful numeric value out of GE's result dict.
    GE puts it in different keys depending on expectation type."""
    for key in ("observed_value", "unexpected_percent",
                "unexpected_count", "element_count"):
        v = result_dict.get(key)
        if isinstance(v, (int, float)):
            return float(v)
    return None


def _extract_threshold(expectation_kwargs: dict):
    """Pull the threshold from the expectation's kwargs."""
    if "mostly" in expectation_kwargs:
        return float(expectation_kwargs["mostly"])
    if "min_value" in expectation_kwargs:
        return float(expectation_kwargs["min_value"])
    if "max_value" in expectation_kwargs:
        return float(expectation_kwargs["max_value"])
    return 0.0


def run_ge_validation(batch_df: DataFrame) -> list[dict]:
    """Run GE 0.18 suite against the batch DataFrame. Return normalized check results."""
    from great_expectations.dataset import SparkDFDataset

    # GE 0.18 has a simple wrapper: SparkDFDataset wraps a Spark DataFrame
    # and exposes all expectations as direct methods on it.
    ds = SparkDFDataset(batch_df)

    out = []
    for exp in SUITE_DICT["expectations"]:
        meta = exp.get("meta", {})
        etype = exp["expectation_type"]
        kwargs = exp["kwargs"]

        # Call the expectation method dynamically
        method = getattr(ds, etype, None)
        if method is None:
            print(f"[ge-skip] unknown expectation: {etype}")
            continue

        try:
            result = method(**kwargs)
            success = result.success
            r = result.result or {}
        except Exception as e:
            print(f"[ge-skip] {etype} failed: {e}")
            continue

        out.append({
            "name":      etype,
            "rule_id":   meta.get("rule_id", "UNKNOWN"),
            "dimension": meta.get("dimension", "unknown"),
            "severity":  meta.get("severity", "warn"),
            "outcome":   "pass" if success else "fail",
            "value":     _extract_value(r),
            "threshold": _extract_threshold(kwargs),
        })
    return out


# ---------------------------------------------------------------------------
def to_obs_event(check: dict, batch_id: int, batch_size: int) -> dict:
    status   = "PASS" if check["outcome"] == "pass" else "BREACH"
    severity = check["severity"].upper() if status == "BREACH" else "INFO"

    return {
        "event_id":     str(uuid.uuid4()),
        "pipeline_id":  "orders-ingestion-pipeline",
        "batch_id":     str(batch_id),
        "entity":       f"kafka.{SOURCE_TOPIC}",
        "metric":       check["name"],
        "dq_dimension": check["dimension"],
        "rule_id":      check["rule_id"],
        "value":        check["value"],
        "threshold":    check["threshold"],
        "status":       status,
        "severity":     severity,
        "timestamp":    datetime.now(timezone.utc).isoformat(),
        "metadata": {
            "batch_size":    batch_size,
            "check_outcome": check["outcome"],
        },
    }


# ---------------------------------------------------------------------------
def build_handler():
    def handler(batch_df: DataFrame, batch_id: int):
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

        if n_clean > 0:
            (clean_df
                .select(F.to_json(F.struct(*clean_df.columns)).alias("value"))
                .write.format("kafka")
                .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
                .option("topic", CLEAN_TOPIC)
                .save())

        if n_quarantine > 0:
            quar_payload = (quarantine_df
                            .withColumn("_batch_id", F.lit(batch_id))
                            .withColumn("_quarantined_at", F.current_timestamp().cast("string")))
            (quar_payload
                .select(F.to_json(F.struct(*quar_payload.columns)).alias("value"))
                .write.format("kafka")
                .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
                .option("topic", QUARANTINE_TOPIC)
                .save())

        # --- 2. Batch-level DQ: Great Expectations --------------------------
        try:
            checks = run_ge_validation(batch_df)
        except Exception as e:
            print(f"[ge-error] batch={batch_id} error={e}")
            checks = []

        for c in checks:
            print(f"[ge] {c['rule_id']:<12} outcome={c['outcome']:<4} "
                  f"value={c['value']} threshold={c['threshold']}")

        events = [to_obs_event(c, batch_id, size) for c in checks]

        if events:
            obs_df = spark.createDataFrame(
                [(json.dumps(e),) for e in events], ["value"]
            )
            (obs_df.write.format("kafka")
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
             .appName("dq_stream_job_ge")
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