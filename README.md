# Real-Time Fraud Signal Pipeline

[![CI](https://github.com/aescal-st/fraud-signal-pipeline/actions/workflows/ci.yml/badge.svg)](https://github.com/aescal-st/fraud-signal-pipeline/actions/workflows/ci.yml)

Streaming fraud detection: Python transaction generator → Kafka → PySpark Structured Streaming → Delta Lake.

## Architecture

```
generator/producer.py --> Kafka (topic: transactions, 6 partitions)
                          |
                          v
           fraud_job.py (PySpark Structured Streaming)
            - JSON parsing with explicit schema
            - 10-min watermark on event_time
            - 5-min sliding windows (1-min slide) per card_id
            - velocity rules: count > 4 OR sum > $400
                          |
            +-------------+-------------+
            v                           v
   Delta bronze table           Delta gold table
   (all raw events)             (fraud alerts only)
```

## Project structure

```
fraud-signal-pipeline/
├── docker-compose.yml          # Kafka in KRaft mode
├── generator/
│   └── producer.py             # fake payment traffic + injected fraud bursts
├── streaming/
│   └── fraud_job.py            # PySpark Structured Streaming job
├── tests/
│   └── test_rules.py           # fraud-rule unit tests
├── .github/workflows/ci.yml    # CI: pytest on every push
├── verify.py                   # check Delta table counts (dev tool)
├── dupe_check.py               # duplicate-alert check (dev tool)
└── README.md
```

## Run it

Prereqs: Docker, Java 11+, Python 3.10+.

```bash
# 1. Start Kafka
docker compose up -d
docker exec kafka /opt/kafka/bin/kafka-topics.sh --create --topic transactions \
  --bootstrap-server localhost:9092 --partitions 6 --replication-factor 1

# 2. Start the transaction generator (leave running)
pip install kafka-python
python generator/producer.py

# 3. Start the streaming job (second terminal, leave running)
pip install pyspark==3.5.0 delta-spark==3.1.0
python streaming/fraud_job.py

# 4. Verify
python verify.py        # bronze/alert counts + sample alerts
python dupe_check.py    # duplicate check (expect 0)
```

## Design decisions

- **6 Kafka partitions** — sets read parallelism; up to 6 Spark tasks consume the topic concurrently.
- **Explicit schema on parse** — malformed events fail loudly instead of silently becoming nulls.
- **10-minute watermark** — accepts data up to 10 min late (correctness), then closes windows so Spark drops old state from memory. Tradeoff: aggregated results lag ~15 min behind real time.
- **Sliding 5-min / 1-min windows** — fraud is bursty; a fixed window could split 6 bad txns 3-and-3 across a boundary and miss both. Overlapping windows catch boundary-straddling bursts.
- **Bronze/gold medallion** — bronze keeps every raw event (replayable, auditable); gold holds only refined alerts.

## Exactly-once

Three mechanisms, tested not assumed:

1. Kafka offsets commit only *after* a micro-batch (including the Delta write) fully succeeds — a crash replays from the last committed offset.
2. Checkpointed state restores window aggregates on restart — replays recompute instead of double-counting.
3. Delta's transaction log makes each batch write atomic — a replayed batch can't half-write.

**Failure test:** killed the streaming job mid-run with the generator still producing, restarted with the same checkpoints. Result: alerts went 740 → 2,787 (backlog replayed, nothing lost) with **0 duplicate alerts** (verified via `dupe_check.py`).

## Tests

`pytest tests/ -v` — unit tests for the fraud-rule logic as a pure function. CI runs on every push.
