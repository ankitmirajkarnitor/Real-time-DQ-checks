# Real-Time Data Quality Observer — Consolidated Guide

End-to-end runbook for the real-time DQ POC: **producer → Kafka → Spark + Soda → observability events → Prometheus → Grafana**.

This document consolidates Phase 0 (foundation), Phase 1 (data producer), Phase 2 (Soda DQ checks in Spark), and Phase 3 (Prometheus + Grafana wiring) into a single guide.

---

## Architecture at a glance

```
┌──────────┐   orders_raw    ┌──────────────┐   observability_events   ┌──────────────────┐   /metrics   ┌────────────┐   queries   ┌─────────┐
│ Producer ├────────────────▶│ Kafka (KRaft)├─────────────────────────▶│ obs-events-      ├─────────────▶│ Prometheus ├────────────▶│ Grafana │
│ (Python) │                 │  + Spark+Soda│                          │ exporter (Py)    │              │            │             │         │
└──────────┘                 └──────────────┘                          └──────────────────┘              └────────────┘             └─────────┘
```

---

## Directory layout

```
real-time-observer-poc/
├── docker-compose.yml
├── producer/
│   ├── orders_producer.py
│   └── requirements.txt
├── jobs/                       # mounted into Spark containers at /opt/jobs
│   ├── smoke_test.py
│   ├── dq_stream_job.py
│   └── dq/
│       └── orders_checks.yml   # Soda Core rules
├── obs-consumer/
│   ├── obs_events_exporter.py
│   ├── requirements.txt
│   └── Dockerfile
├── prometheus/
│   └── prometheus.yml
├── grafana/
│   ├── provisioning/
│   │   ├── datasources/prometheus.yml
│   │   └── dashboards/dashboards.yml
│   └── dashboards/
│       └── dq_overview.json
└── data/
```

---

## Service URLs

| Service | URL | Notes |
|---|---|---|
| Redpanda Console | http://localhost:8080 | Kafka topic browser |
| Schema Registry | http://localhost:8081 | |
| Spark Master UI | http://localhost:8090 | |
| Spark Worker UI | http://localhost:8091 | |
| Kafka (from host) | `localhost:9094` | |
| Kafka (inside Docker) | `kafka:9092` | |
| obs-events-exporter | http://localhost:9108/metrics | Prometheus scrape target |
| Prometheus | http://localhost:9090 | |
| Grafana | http://localhost:3000 | login: `admin` / `admin` |

---

## Phase 0 — Foundation (stack)

### Image / version baseline (v2)

| Area | Value |
|---|---|
| Spark image | `apache/spark:4.0.2-python3` |
| Scala | 2.13 |
| Kafka broker image | `apache/kafka:3.9.0` (**KRaft only**, no Zookeeper) |
| UI | `redpanda-data/console` |
| Spark command | explicit `spark-class` invocation |
| Ivy cache path | `/root/.ivy2` (since `user: root`) |

### Start the stack

```powershell
mkdir jobs, data -ErrorAction SilentlyContinue
docker compose up -d
docker compose ps
```

Wait until `kafka` and `schema-registry` show `(healthy)`.

### Create topics

```powershell
docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --create --topic orders_raw --partitions 3 --replication-factor 1 --if-not-exists
docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --create --topic orders_clean --partitions 3 --replication-factor 1 --if-not-exists
docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --create --topic orders_quarantine --partitions 3 --replication-factor 1 --if-not-exists
docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --create --topic observability_events --partitions 3 --replication-factor 1 --if-not-exists
```

> Binary path is `/opt/kafka/bin/kafka-topics.sh` (Apache image), not `kafka-topics` without `.sh` (Confluent image).

### Smoke test (`jobs/smoke_test.py`)

```python
from pyspark.sql import SparkSession

spark = (SparkSession.builder
         .appName("smoke_test")
         .getOrCreate())
spark.sparkContext.setLogLevel("WARN")

df = (spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "kafka:9092")
      .option("subscribe", "orders_raw")
      .option("startingOffsets", "earliest")
      .load())

(df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)", "timestamp")
   .writeStream
   .format("console")
   .outputMode("append")
   .option("truncate", "false")
   .start()
   .awaitTermination())
```

Run it (Spark 4.0.2 → Scala 2.13 → Kafka connector suffix `_2.13`):

```powershell
docker exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.2 /opt/jobs/smoke_test.py
```

First run downloads transitive deps (~2 min). Cached in `spark-ivy-cache` afterwards.

### Produce a test message

```powershell
docker exec -it kafka /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server kafka:9092 --topic orders_raw
> {"order_id":"1","customer_id":"c-101","amount":42}
> {"order_id":"2","customer_id":"c-102","amount":99}
```

The Spark console-sink stream should print both rows within seconds.

---

## Phase 1 — Run the producer (on host)

### One-time setup

```powershell
cd producer
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

### Baseline — ~500 records / 60 s (≈ 8.3 rps)

```powershell
python orders_producer.py --rps 8.3 --duration 60
```

### With realistic DQ failures injected

```powershell
python orders_producer.py --rps 8.3 --duration 120 `
  --null-rate 0.05 --dup-rate 0.02 --late-rate 0.01 `
  --drift-rate 0.005 --email-bad-rate 0.03
```

### Scale — 1000 rps for 10 s

```powershell
python orders_producer.py --rps 1000 --duration 10
```

### Scale with spikes — baseline 200 rps, 10× spike every 30 s

```powershell
python orders_producer.py --rps 200 --duration 300 `
  --spike-mult 10 --spike-every 30 --spike-duration 5 `
  --null-rate 0.03 --dup-rate 0.01
```

Verify in Redpanda Console (http://localhost:8080) → **Topics** → `orders_raw`.

---

## Phase 2 — Soda Core DQ checks in Spark

### What this does

Spark Structured Streaming job that:
1. Reads `orders_raw` from Kafka.
2. Every 30 seconds, takes the micro-batch as a Spark DataFrame.
3. Runs Soda Core checks declared in `dq/orders_checks.yml`.
4. Emits an observability event per check result to `observability_events`.

### Install Soda Core into the Spark containers

Soda isn't pre-installed in `apache/spark:4.0.2-python3`:

```powershell
docker exec spark-master pip install --break-system-packages soda-core-spark-df==3.5.5
docker exec spark-worker pip install --break-system-packages soda-core-spark-df==3.5.5
```

> For anything beyond the POC, bake this into a custom Dockerfile instead of `pip install` at runtime.

### Run the DQ stream job (single line PowerShell)

```powershell
docker exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.2 --conf spark.sql.streaming.checkpointLocation=/opt/checkpoints/dq_stream_job /opt/jobs/dq_stream_job.py
```

### While it's running, fire bad data from a second window

```powershell
cd producer
.\.venv\Scripts\Activate.ps1
python orders_producer.py --rps 20 --duration 180 --null-rate 0.10 --dup-rate 0.03
```

### Observability event shape

In Redpanda Console → `observability_events`:

```json
{
  "event_id": "…",
  "pipeline_id": "orders-ingestion-pipeline",
  "batch_id": "3",
  "entity": "kafka.orders_raw",
  "metric": "Completeness – customer_id null percentage under 5%",
  "dq_dimension": "completeness",
  "rule_id": "R-COMPL-001",
  "value": 9.8,
  "threshold": 5,
  "status": "BREACH",
  "severity": "WARN",
  "timestamp": "2026-04-22T...",
  "metadata": { "batch_size": 402, "check_outcome": "fail" }
}
```

This closes the loop: **producer → Kafka → Spark + Soda → observability events**.

---

## Phase 3 — Prometheus + Grafana wiring

### Step 1 — Merge the three new services into `docker-compose.yml`

1. **Append** `obs-events-exporter`, `prometheus`, `grafana` from `docker-compose.phase3-snippet.yml` into the `services:` block.
2. **Add** these to the existing `volumes:` block:
   ```yaml
   volumes:
     kafka-data:
     spark-ivy-cache:
     spark-checkpoints:
     prometheus-data:      # ← new
     grafana-data:         # ← new
   ```

### Step 2 — Build and start

```powershell
docker compose build obs-events-exporter
docker compose up -d obs-events-exporter prometheus grafana
docker compose ps
```

If `obs-events-exporter` exits:

```powershell
docker compose logs obs-events-exporter --tail 30
```

### Step 3 — Verify each layer independently

**3.1 Exporter →** `curl http://localhost:9108/metrics`

Initially (no events yet):
```
# HELP dq_consumer_up ...
dq_consumer_up 1.0
# HELP dq_events_total ...
# (no samples yet)
```

**3.2 Prometheus →** http://localhost:9090 → **Status → Targets** → `obs-events-exporter` should be `UP`. Quick query: `dq_consumer_up` returns `1`.

**3.3 Grafana →** http://localhost:3000 (`admin` / `admin`) → **Data Quality → Real-Time Data Quality Observability** dashboard auto-loads. Panels show "No data" until events flow.

### Step 4 — Light up the pipeline

Start the DQ stream job (if not already running):

```powershell
docker exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.2 /opt/jobs/dq_stream_job.py
```

Bad-data producer in another window:

```powershell
python orders_producer.py --rps 10 --duration 180 --null-rate 0.08 --dup-rate 0.03 --email-bad-rate 0.05
```

Within 30–60 s:

| Layer | What to check |
|---|---|
| `curl http://localhost:9108/metrics` | `dq_events_total{...}`, `dq_metric_value{...}` populated |
| http://localhost:9090 → `dq_events_total` | series per `{rule_id, status, severity}` |
| Grafana dashboard | DQ Score drops, Breach rate timeseries shows activity |

### Step 5 — Reading the dashboard panels

| Panel | What it tells you |
|---|---|
| **DQ Score (100 − %breaches)** | Single KPI. 100 = all checks passing in last 5m. Green ≥ 95, orange 80–95, red < 80. |
| **Alerts Triggered (5m)** | Count of BREACH events in rolling 5-minute window. |
| **Critical Breaches (5m)** | Same but filtered to `severity=CRITICAL` — the pageable ones. |
| **Consumer Up** | Canary: `1` = exporter connected to Kafka, `0` = pipeline blind. |
| **Null % (customer_id)** | Time series for `R-COMPL-001` — measured value with threshold line overlay. |
| **Breach rate by dimension** | Disaggregated breach/sec by `completeness / uniqueness / validity / volume`. |
| **Events by Rule (last 15m)** | Ranks rules by chattiness — useful for alert-fatigue analysis. |
| **Latest value per rule** | Snapshot table — most recent measurement for every rule. |

### Step 6 — Useful PromQL queries

```promql
# Current DQ score
100 * (1 - sum(rate(dq_events_total{status="BREACH"}[5m]))
            / clamp_min(sum(rate(dq_events_total[5m])), 1))

# Breaches per rule per minute
sum by (rule_id) (rate(dq_events_total{status="BREACH"}[1m])) * 60

# Which rules have breached in the last 15m
count by (rule_id) (increase(dq_events_total{status="BREACH"}[15m]) > 0)

# How fresh is our observability data (seconds since last event)
time() - dq_last_event_timestamp_seconds
```

---

## End-to-end sanity checklist

**Phase 0/1 — Foundation & producer**
- [ ] `docker compose ps` shows all base services healthy
- [ ] Producer baseline (8.3 rps × 60 s) lands ~500 messages in `orders_raw`
- [ ] Producer scale test (1000 rps × 10 s) lands ~10 000 messages without errors

**Phase 2 — Soda DQ checks**
- [ ] Soda Core installed in both `spark-master` and `spark-worker`
- [ ] `dq_stream_job.py` runs without exceptions, prints `[dq] batch=N size=M checks=K breaches=L`
- [ ] `observability_events` shows events with `status=PASS` under clean traffic
- [ ] `--null-rate 0.10` flips `R-COMPL-001` to `status=BREACH`

**Phase 3 — Prom + Grafana**
- [ ] `obs-events-exporter` stays up across multiple producer runs
- [ ] Prometheus **Targets** shows the exporter as `UP` continuously
- [ ] Clean traffic → DQ Score ≈ 100, no red panels
- [ ] `--null-rate 0.10` drives DQ Score down and Null% panel above threshold
- [ ] Critical Breaches panel ≥ 1 on `--dup-rate 0.03`
- [ ] `time() - dq_last_event_timestamp_seconds` stays under 60s during active runs

---

## Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| `NoClassDefFoundError: scala/...` on submit | Using `_2.12` package with Spark 4.0 | Use `_2.13:4.0.2` |
| Kafka exits with `InconsistentClusterIdException` | Stale data volume from v1 | `docker compose down -v` then `up -d` |
| Redpanda Console shows "no brokers" | Kafka not healthy yet at startup | Wait for `kafka` healthcheck, then `docker compose restart redpanda-console` |
| Worker memory warning (≤1G) | Default inherited | Confirm `SPARK_WORKER_MEMORY: "2G"` in compose |
| Permission denied writing to `/opt/jobs` | Host dir owned by your UID, Spark runs as root | With `user: root` in compose, writes go in as root — acceptable for POC |
| `obs-events-exporter` exits on boot | Kafka not reachable, or topic missing | Check `docker compose logs obs-events-exporter --tail 30`; confirm `observability_events` exists |
| Grafana panels stay "No data" | No events yet, or Prometheus not scraping | Check `dq_events_total` in Prom UI; check **Status → Targets** |

---

## Roadmap — what's NOT yet implemented

Deferred to later phases:

1. **Alertmanager routing to Slack/PagerDuty** — Prometheus has the data, no routing policy yet.
2. **Postgres/ClickHouse historical sink** — for weekly/monthly trend reports and PowerBI. Add as a second consumer in Phase 4.
3. **Per-column metrics** — currently aggregated at the rule level. Adding column-level granularity is a cardinality decision best made after seeing real traffic.
4. **Quarantine & clean topics** — route bad records to `orders_quarantine`, clean records to `orders_clean`.
5. **Schema Registry enforcement** — swap JSON for Avro, block malformed at the producer.
