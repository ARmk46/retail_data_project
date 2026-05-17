import json
import sys
from confluent_kafka import Consumer, KafkaError, KafkaException

# Kafka Configuration
kafka_config = {
    
    'bootstrap.servers': '127.0.0.1:9092',
    'group.id': 'retail-group-xray-1',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': True,
    
    # --- NEW DEBUGGING TOOLS ---
    #'debug': 'broker,cgrp,topic', 
    #'error_cb': lambda err: print(f"\n🚨 KAFKA FATAL ERROR: {err}\n")
}


# Initialize Consumer
consumer = Consumer(kafka_config)
TOPIC_NAME = 'retail_orders'

# Subscribe to the topic
consumer.subscribe([TOPIC_NAME])

print(f"🎧 Consumer group '{kafka_config['group.id']}' listening to topic: '{TOPIC_NAME}'...")
print("Press Ctrl+C to stop.")

try:
    while True:
        # 1. Poll Kafka for new messages (wait up to 1 second)
        msg = consumer.poll(timeout=1.0)
        
        # 2. Handle empty polls
        if msg is None:
            continue
            
        # 3. Handle Kafka errors
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                # End of partition event (not an actual error, just an informational flag)
                continue
            elif msg.error():
                raise KafkaException(msg.error())
                
        # 4. Extract and deserialize the payload
        try:
            # Decode the key and value from bytes back to strings, then parse the JSON
            order_id = msg.key().decode('utf-8')
            payload = json.loads(msg.value().decode('utf-8'))
            
            # Print the received data in a clean format
            print(f"📥 [RECEIVED] Partition: {msg.partition()} | Order: {order_id}")
            print(f"   ↳ {payload['quantity']}x {payload['product_category']} | ${payload['price']} | {payload['payment_method']}")
            print("-" * 50)
            
        except json.JSONDecodeError:
            print(f"⚠️ Failed to decode message value: {msg.value()}")

except KeyboardInterrupt:
    print("\n🛑 Consumer stopped by user.")
finally:
    # Always close the consumer cleanly to release partition locks
    consumer.close()
    print("👋 Connection closed.")