from kafka import KafkaConsumer
import json

print("________________________________________________________________")
print("             CRITICAL FAILURE SYSTEM MONITOR (DLQ)              ")
print("________________________________________________________________\n")

consumer = KafkaConsumer(
    'failed-orders',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    group_id='dlq-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

for msg in consumer:
    o = msg.value
    print("________________________________________________________________")
    print(f"[ALERT] Processing Stopped for Order #{o['order_id']}")
    print(f"[REASON] Failure Flag: {o.get('reason')}")
    print(f"[PAYLOAD] Client: {o['customer']:<6} | Item: {o['item']:<8} | Quantity: {o['qty']}")
    print("________________________________________________________________\n")