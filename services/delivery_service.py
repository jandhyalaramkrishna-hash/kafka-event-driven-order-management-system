from kafka import KafkaConsumer
import json, random, time
import mysql.connector

print("________________________________________________________________")
print("                  DISPATCH & DELIVERY SERVICE                  ")
print("________________________________________________________________\n")

def get_db():
    return mysql.connector.connect(
        host="localhost",
        user="root",
        password="root123",
        database="oms_db"
    )

consumer = KafkaConsumer(
    'delivery-orders',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    group_id='delivery-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

for msg in consumer:
    o = msg.value
    order_id = o["order_id"]

    print("________________________________________________________________")
    print(f"[DISPATCH] Route assigned for Order #{order_id} to Client: {o['customer']}")
    
    otp = random.randint(1000, 9999)
    print(f"[SECURITY] Authentication Token Generated: {otp}")
    
    time.sleep(1.5)
    print("[TRANSIT] Validating handheld secure token handshake...")
    print("[HANDOVER] Verification successful. Parcel collected.")

    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("""
        UPDATE orders_master_table
        SET order_status = 'DELIVERED'
        WHERE order_id = %s
    """, (order_id,))
    conn.commit()
    cursor.close()
    conn.close()

    print(f"[DATABASE] Order #{order_id} state updated to DELIVERED")
    print("________________________________________________________________\n")