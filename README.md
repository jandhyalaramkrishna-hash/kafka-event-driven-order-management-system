# 📦 FoodSupply Engine – Event Driven Order Management System (OMS)

---

## 🚀 Project Overview

FoodSupply Engine is a **real-time event-driven Order Management System** built using **Kafka-based microservices 
architecture**.

This project simulates a **restaurant order lifecycle**, handling:

* Order ingestion
* Duplicate detection
* Payment validation
* Kitchen processing
* Inventory management
* Delivery orchestration
* Dead Letter Queue (DLQ) handling

---

## 🧠 Architecture Flow

```
Order API → Kafka → Order Service → Kitchen → Delivery
                          ↓
                      DLQ Service
                          ↓
                Duplicate Detection Service
```

---

## 🛠️ Tech Stack

* Python (Core Services)
* Apache Kafka (Event Streaming)
* Zookeeper (Kafka Coordination)
* FastAPI (Order API)
* MySQL (Optional / Partial usage)
* Docker (Kafka + MySQL setup)
* PowerShell (Execution)
* Newman / Allure (Optional Reporting)
* Grafana (Monitoring - optional)

---

## ⚙️ Services Overview

### 1. Order Service (Orchestrator)

* Receives orders
* Validates payment
* Detects duplicates
* Sends valid orders to kitchen
* Routes failed orders to DLQ

---

### 2. Kitchen & Inventory Engine

* Processes valid orders
* Checks stock availability
* Updates inventory
* Rejects out-of-stock orders
* Sends READY orders to delivery

---

### 3. Delivery Service

* Handles only READY orders
* Generates OTP
* Simulates secure delivery
* Marks orders as DELIVERED

⚠️ Important:
Only processes:

```
status = READY
```

---

### 4. Dead Letter Queue (DLQ)

* Captures failed orders
* Payment failures
* Inventory failures
* Prevents bad data from entering system

---

### 5. Duplicate Monitor

* Detects identical orders
* Blocks duplicate transactions
* Logs duplicate activity

---

## 📊 Sample Order Flow

| Order ID | Status              	 	| Flow Result  
| -------- | ------------------------------------------------ | ----------------  
| 1        | SUCCESS              		| Delivered         
| 2        | Payment Failed     		| Sent to DLQ       
| 3        | SUCCESS            		| Delivered        
| 4        | Duplicate         		| Blocked           
| 5        | SUCCESS / Stock Fail 	| Depends on stock 
| 6        | Out of Stock        		| Sent to DLQ       
| 7        | Duplicate          		| Blocked         
| 8        | Out of Stock   		| Sent to DLQ     

---

## 🔁 Key Features

✔ Event-driven architecture
✔ Kafka-based messaging
✔ Idempotency (Duplicate prevention)
✔ Dead Letter Queue (DLQ)
✔ Inventory validation
✔ Real-time processing simulation
✔ Clean service separation

---

## 🧪 How to Run (Single Flow)

### Step 1: Start Docker

```
docker start qa-zookeeper qa-kafka qa-mysql
```

---

### Step 2: Run API

```
uvicorn app.main:app --reload
```

Open:

```
http://127.0.0.1:8000/orders
```

---

### Step 3: Run Services (Separate Windows)

```
python services/order_service.py
python services/kitchen_service.py
python services/delivery_service.py
python services/dlq_service.py
python services/duplicate_service.py
```

---

## ⚠️ Important Notes

* Delivery service must NOT process FAILED orders
* DLQ ensures system reliability
* Inventory controls kitchen acceptance
* Duplicate service ensures idempotency

---

## 📈 Monitoring & Reporting (Optional)

* Newman → API testing
* Allure → Test reports
* Grafana → Visualization dashboard

---

## 🎯 Learning Outcome

This project demonstrates:

* Microservices architecture
* Kafka event streaming
* System reliability patterns (DLQ)
* Real-world OMS flow
* Debugging distributed systems

---

## 🧑‍💻 Author

Developed as a **hands-on enterprise-level QA + backend simulation project**
Focused on real-time system understanding and interview readiness.

---

## 🔥 Final Note

This is not just a demo project.

It represents:

* Real-world system thinking
* Event-driven design
* Production-like architecture simulation

---

END OF README
