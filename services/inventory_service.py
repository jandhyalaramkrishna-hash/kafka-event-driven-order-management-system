from kafka import KafkaConsumer, KafkaProducer
import json
import time
import mysql.connector

print("\n========= KITCHEN =========")

def get_db():
    return mysql.connector.connect(
        host="localhost",
        user="root",
        password="root123",
        database="oms_db"
    )

# ✅ CORRECT TOPIC
consumer = KafkaConsumer(
    'validated-orders',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    group_id='kitchen-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

producer_delivery = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

producer_dlq = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

for msg in consumer:
    o = msg.value

    print(f"\nRECEIVED IN KITCHEN: {o}")

    if o["payment"] != "SUCCESS":
        continue

    conn = get_db()
    cursor = conn.cursor()

    cursor.execute(
        "SELECT remaining FROM items WHERE item_name=%s",
        (o["item"],)
    )
    result = cursor.fetchone()

    if result and result[0] >= o["qty"]:

        print(f"Order {o['order_id']} cooking {o['item']}")
        time.sleep(2)
        print(f"Order {o['order_id']} ready")

        cursor.execute("""
            UPDATE items
            SET remaining = remaining - %s,
                used = used + %s
            WHERE item_name = %s
        """, (o["qty"], o["qty"], o["item"]))

        conn.commit()

        producer_delivery.send("delivery-orders", o)

    else:
        print(f"Order {o['order_id']} rejected → Not enough stock")

        producer_dlq.send("dlq-orders", {
            **o,
            "reason": "Out of Stock"
        })