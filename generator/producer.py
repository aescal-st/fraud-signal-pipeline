import json, random, time, uuid
from datetime import datetime, timezone
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    acks="all",
)
CARDS = [f"card_{i:04d}" for i in range(200)]
MERCHANTS = [f"m_{i:03d}" for i in range(40)]

def txn(card_id, amount):
    return {
        "transaction_id": str(uuid.uuid4()),
        "card_id": card_id,
        "merchant_id": random.choice(MERCHANTS),
        "amount": round(amount, 2),
        "event_time": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
    }

def fraud_burst():
    card = random.choice(CARDS)
    for _ in range(random.randint(5, 8)):
        producer.send("transactions", txn(card, random.uniform(40, 95)))
    producer.flush()

n = 0
while True:
    n += 1
    if n % 25 == 0:
        fraud_burst()
    else:
        producer.send("transactions", txn(random.choice(CARDS), random.uniform(5, 250)))
    if n % 50 == 0:
        producer.flush()
        print(f"sent {n}", flush=True)
    time.sleep(0.2)
