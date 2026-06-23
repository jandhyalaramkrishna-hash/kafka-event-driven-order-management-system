from fastapi import FastAPI
import json
import mysql.connector

# This line must be exactly here for Uvicorn to work!
app = FastAPI()

def get_db():
    return mysql.connector.connect(
        host="localhost",
        user="root",
        password="root123",
        database="oms_db"
    )

producer = None

def get_producer():
    global producer
    if producer is None:
        from kafka import KafkaProducer
        producer = KafkaProducer(
            bootstrap_servers='localhost:9092',
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
    return producer

# PERFECT 8-ORDER SCENARIO: 3 Pass, 3 Fail (DLQ), 2 Duplicate Blocks
orders = [
    {"order_id": 1, "customer": "Ram", "item": "Biryani", "qty": 2, "amount": 500, "payment": "SUCCESS"},
    {"order_id": 2, "customer": "Ravi", "item": "Dosa", "qty": 1, "amount": 100, "payment": "FAILED"},
    {"order_id": 3, "customer": "Sita", "item": "Idli", "qty": 2, "amount": 150, "payment": "SUCCESS"},
    {"order_id": 4, "customer": "Ram", "item": "Biryani", "qty": 2, "amount": 500, "payment": "SUCCESS"},
    {"order_id": 5, "customer": "Vikram", "item": "Ghost Pepper Chai", "qty": 1, "amount": 90, "payment": "SUCCESS"},
    {"order_id": 6, "customer": "John", "item": "Sleepy Noodles", "qty": 5, "amount": 450, "payment": "SUCCESS"},
    {"order_id": 7, "customer": "Sita", "item": "Idli", "qty": 2, "amount": 150, "payment": "SUCCESS"},
    {"order_id": 8, "customer": "Anjali", "item": "Zero Calories Pizza", "qty": 2, "amount": 300, "payment": "SUCCESS"}
]

def process_orders():
    print("\n________________________________________________________________")
    print("                ORDER MANAGEMENT ENGINE INGESTION               ")
    print("________________________________________________________________\n")

    conn = get_db()
    cursor = conn.cursor()

    try:
        cursor.execute("DELETE FROM orders_master_table")
        conn.commit()
    except Exception as e:
        print(f"Could not clear old entries: {e}")

    for o in orders:
        try:
            cursor.execute("""
                INSERT INTO orders_master_table
                (order_id, customer_name, item, qty, amount, order_status)
                VALUES (%s, %s, %s, %s, %s, 'PENDING')
            """, (
                o["order_id"],
                o["customer"],
                o["item"],
                o["qty"],
                o["amount"]
            ))
            conn.commit()
            print(f"[DATABASE] Order ID {o['order_id']:<3} | Customer: {o['customer']:<6} | Status: STAGED (PENDING)")

        except Exception as e:
            print(f"[DB ERROR] Order ID {o['order_id']} Failed insertion -> {e}")

        get_producer().send("food-orders", o)

    cursor.close()
    conn.close()
    print("Orders sent to Kafka\n")

@app.get("/orders")
def trigger():
    process_orders()
    return {"status": "Orders Sent"}