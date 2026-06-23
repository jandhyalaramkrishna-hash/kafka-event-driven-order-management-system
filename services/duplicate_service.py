from kafka import KafkaConsumer
import json

print("________________________________________________________________")
print("                  DUPLICATE TRANSACTION MONITOR                 ")
print("________________________________________________________________\n")

consumer = KafkaConsumer(
    'duplicate-orders',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    group_id='duplicate-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

for msg in consumer:
    o = msg.value
    print("________________________________________________________________")
    print(f"[DUPLICATE BLOCKED] Safety Rule Triggered for Order #{o['order_id']}")
    print(f"[REASON] Exact same order already exists in system")
    print(f"[PAYLOAD] Client: {o['customer']:<6} | Item: {o['item']:<8} | Quantity: {o['qty']}")
    print("________________________________________________________________\n")