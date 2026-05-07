import json, time, uuid, random
from datetime import datetime
from kafka import KafkaProducer
from config.settings import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC

MERCHANTS  = ["Amazon", "Tesco", "Monzo", "Netflix", "Uber", "Deliveroo"]
CURRENCIES = ["GBP", "USD", "EUR"]
STATUSES   = ["completed", "pending", "failed"]

def generate_transaction():
    return {
        "transaction_id": str(uuid.uuid4()),
        "user_id":        f"user_{random.randint(1, 1000)}",
        "amount":         round(random.uniform(1.0, 2000.0), 2),
        "currency":       random.choice(CURRENCIES),
        "merchant":       random.choice(MERCHANTS),
        "timestamp":      datetime.utcnow().isoformat(),
        "status":         random.choices(STATUSES, weights=[80, 15, 5])[0]
    }

def run():
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )
    print("Producer running — sending transactions to Kafka...")
    while True:
        tx = generate_transaction()
        producer.send(KAFKA_TOPIC, tx)
        print(f"Sent: {tx['transaction_id']} | £{tx['amount']} | {tx['merchant']}")
        time.sleep(random.uniform(0.5, 2.0))  # simulate realistic rate

if __name__ == "__main__":
    run()