from kafka import KafkaConsumer, KafkaProducer
import json

print("________________________________________________________________")
print("                   ORDER VERIFICATION SERVICE                   ")
print("________________________________________________________________\n")

consumer = KafkaConsumer(
    'food-orders',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    group_id='order-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

seen = set()

for msg in consumer:
    o = msg.value
    order_id = o["order_id"]
    
    print("________________________________________________________________")
    print(f"[INGEST] Verifying Inbound Order #{order_id}...")

    key = f"{o['customer']}_{o['item']}_{o['qty']}"

    if key in seen:
        print(f"[DUPLICATE] Order #{order_id} -> BLOCKED (Identical record exists)")
        print("________________________________________________________________\n")
        producer.send("duplicate-orders", o)
        continue

    seen.add(key)

    if o["payment"] != "SUCCESS":
        print(f"[PAYMENT FAILED] Order #{order_id} -> Sent to Dead Letter Queue")
        print("________________________________________________________________\n")
        o["reason"] = "Payment Failed"
        producer.send("failed-orders", o)
        continue

    # Changed words to eliminate the stock confusion:
    print(f"[PROCESSED] Order #{order_id} -> Sent to Kitchen for Stock Check")
    print("________________________________________________________________\n")
    producer.send("validated-orders", o)