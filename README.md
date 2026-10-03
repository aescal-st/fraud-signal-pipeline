\# Real-Time Fraud Signal Pipeline



Streaming fraud detection: Python transaction generator → Kafka → PySpark Structured Streaming → Delta Lake.



\## Architecture



```

generator/producer.py --> Kafka (topic: transactions, 6 partitions)

&#x20;                             |

&#x20;                             v

&#x20;             fraud\_job.py (PySpark Structured Streaming)

&#x20;              - JSON parsing with explicit schema

&#x20;              - 10-min watermark on event\_time

&#x20;              - 5-min sliding windows (1-min slide) per card\_id

&#x20;              - velocity rules: count > 4 OR sum > $400

&#x20;                             |

&#x20;                +------------+------------+

&#x20;                v                         v

&#x20;       Delta bronze table          Delta gold table

&#x20;       (all raw events)            (fraud alerts only)

```





\## Run it



Prereqs: Docker, Java 11+, Python 3.10+.



```bash

\# 1. Start Kafka

docker compose up -d

docker exec kafka /opt/kafka/bin/kafka-topics.sh --create --topic transactions \\

&#x20; --bootstrap-server localhost:9092 --partitions 6 --replication-factor 1



\# 2. Start the transaction generator (leave running)

pip install kafka-python

python generator/producer.py



\# 3. Start the streaming job (second terminal, leave running)

pip install pyspark==3.5.0 delta-spark==3.1.0

python streaming/fraud\_job.py



\# 4. Verify

python verify.py        # bronze/alert counts + sample alerts

python dupe\_check.py    # duplicate check (expect 0)





Design decisions

6 Kafka partitions — sets read parallelism; up to 6 Spark tasks consume the topic concurrently.

Explicit schema on parse — malformed events fail loudly instead of silently becoming nulls.

10-minute watermark — accepts data up to 10 min late (correctness), then closes windows so Spark drops old state from memory. Tradeoff: aggregated results lag \~15 min behind real time.

Sliding 5-min / 1-min windows — fraud is bursty; a fixed window could split 6 bad txns 3-and-3 across a boundary and miss both. Overlapping windows catch boundary-straddling bursts.

Bronze/gold medallion — bronze keeps every raw event (replayable, auditable); gold holds only refined alerts.



Exactly-once

Three mechanisms, tested not assumed:



Kafka offsets commit only after a micro-batch (including the Delta write) fully succeeds — a crash replays from the last committed offset.

Checkpointed state restores window aggregates on restart — replays recompute instead of double-counting.

Delta's transaction log makes each batch write atomic — a replayed batch can't half-write.

Failure test: killed the streaming job mid-run with the generator still producing, restarted with the same checkpoints. Result: alerts went 740 → 2,787 (backlog replayed, nothing lost) with 0 duplicate alerts (verified via dupe\_check.py).



Tests

pytest tests/ -v — unit tests for the fraud-rule logic as a pure function. CI runs on every push via .github/workflows/ci.yml.



