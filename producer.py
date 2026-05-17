import json
import random
import time
from datetime import datetime
from faker import Faker
from confluent_kafka import Producer

# Initialize Faker for high-quality mock retail data
fake = Faker()

# Kafka Configuration pointing to your local Docker broker
kafka_config = {
    'bootstrap.servers': '127.0.0.1:9092',
    'client.id': 'retail-transaction-producer',
    'acks': 'all',                      # Guarantee data durability (no data loss)
    'linger.ms': 10,                    # Tiny delay to batch messages together for throughput
    'compression.type': 'snappy'        # Compress batches to save network bandwidth
}

# Initialize the Kafka Producer
producer = Producer(kafka_config)
TOPIC_NAME = 'retail_orders'

def delivery_report(err, msg):
    """ Callback invoked by Kafka when a message is successfully delivered or fails. """
    if err is not None:
        print(f"❌ Delivery failed for record {msg.key()}: {err}")
    else:
        print(f"✅ Record {msg.key().decode('utf-8')} securely sent to partition [{msg.partition()}]")

def generate_retail_event():
    """ Generates a single mock retail transaction event. """
    order_id = f"ORD-{fake.unique.random_number(digits=8)}"
    user_id = f"USR-{random.randint(1000, 9999)}"
    
    event = {
        "order_id": order_id,
        "user_id": user_id,
        "timestamp": datetime.utcnow().isoformat(),
        "product_category": random.choice(["Electronics", "Apparel", "Home & Kitchen", "Books", "Beauty", "Sports"]),
        "quantity": random.randint(1, 5),
        "price": round(random.uniform(10.50, 850.00), 2),
        "payment_method": random.choice(["Credit Card", "PayPal", "Apple Pay", "Google Pay"]),
        "country": "India"
    }
    return order_id, event

print(f"🚀 Starting live retail stream to topic: '{TOPIC_NAME}'... Press Ctrl+C to stop.")

try:
    while True:
        # 1. Generate a unique transactional payload
        order_id, payload = generate_retail_event()
        
        # 2. Serialize the Python dictionary into a JSON formatted string
        json_payload = json.dumps(payload).encode('utf-8')
        
        # 3. Produce message to Kafka (asynchronously)
        # Using order_id as the message key guarantees all updates for a specific order land in the same partition
        producer.produce(
            topic=TOPIC_NAME,
            key=order_id.encode('utf-8'),
            value=json_payload,
            callback=delivery_report
        )
        
        # 4. Flush internal buffer periodically to push messages out
        producer.poll(0)
        
        # 5. Sleep for a random fraction of a second to simulate organic human traffic
        time.sleep(random.uniform(0.2, 1.5))

except KeyboardInterrupt:
    print("\n🛑 Ingestion stopped by user. Flushing remaining messages...")
finally:
    # Force any remaining messages in the buffer to be sent before the script exits
    producer.flush(timeout=5)
    print("👋 Shutdown complete.")