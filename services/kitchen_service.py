from kafka import KafkaConsumer, KafkaProducer
import json
import time
import mysql.connector

print("________________________________________________________________")
print("                    KITCHEN & LIVE INVENTORY ENGINE             ")
print("________________________________________________________________\n")

def get_db():
    return mysql.connector.connect(
        host="localhost",
        user="root",
        password="root123",
        database="oms_db"
    )

def print_live_stock_table():
    try:
        conn = get_db()
        cursor = conn.cursor()
        cursor.execute("SELECT item_name, total, remaining, used FROM items")
        rows = cursor.fetchall()
        
        # Widened columns to perfectly fit the new funny names
        print("\n+------------------------------------------------────────+")
        print("|                LIVE STOCK STATUS TABLE                 |")
        print("+----------------------+-----------+-----------+---------+")
        print(f"| {'Item':<20} | {'Total':<9} | {'Remaining':<9} | {'Used':<7} |")
        print("+----------------------+-----------+-----------+---------+")
        for row in rows:
            print(f"| {row[0]:<20} | {row[1]:<9} | {row[2]:<9} | {row[3]:<7} |")
        print("+----------------------+-----------+-----------+---------+\n")
        
        cursor.close()
        conn.close()
    except Exception as e:
        print(f"Error pulling live table visualization: {e}")

consumer = KafkaConsumer(
    'validated-orders',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    group_id='kitchen-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

print_live_stock_table()

for msg in consumer:
    o = msg.value
    order_id = o["order_id"]
    print(f"[INGESTION] Order #{order_id} -> {o['qty']}x {o['item']} for {o['customer']}")

    conn = get_db()
    cursor = conn.cursor()

    cursor.execute("SELECT remaining FROM items WHERE item_name=%s", (o["item"],))
    result = cursor.fetchone()

    if result and result[0] >= o["qty"]:
        print(f"[PROCESSING] Kitchen is preparing order #{order_id}...")
        time.sleep(1.5)

        cursor.execute("""
            UPDATE items
            SET remaining = remaining - %s,
                used = used + %s
            WHERE item_name = %s
        """, (o["qty"], o["qty"], o["item"]))
        conn.commit()
        
        print(f"[SUCCESS] Order #{order_id} marked as READY. Dispatched to delivery stream.")
        
        # ✅ FIX: Explicitly mark order as READY before pushing to delivery pipeline
        o["status"] = "READY"
        producer.send("delivery-orders", o)
    else:
        print(f"[REJECTED] Order #{order_id} -> Stock count insufficient for '{o['item']}'")
        
        # ✅ FIX: Explicitly mark order as FAILED and isolate it from the delivery topic entirely
        o["status"] = "FAILED"
        o["reason"] = "Out of Stock"
        producer.send("failed-orders", o)

    cursor.close()
    conn.close()
    print_live_stock_table()