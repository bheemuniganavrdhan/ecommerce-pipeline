# 🛒 Real-Time E-Commerce Streaming Telemetry Pipeline

**JNTUK B.Tech Curriculum Project | Big Data Analytics & Data Engineering**

---

### 📌 Problem Statement & Context
Modern e-commerce platforms process millions of concurrent user transactions and clickstream events. Batch processing results in high latency, preventing real-time anomaly detection, flash-sale telemetry tracking, and dynamic pricing decisions. This project establishes an enterprise-grade, event-driven streaming pipeline capable of ingesting, processing, persisting, and visualizing high-throughput e-commerce transaction logs in real time.

---

### 📁 Project Directory Structure
```text
ecommerce-pipeline/
│
├── docker-compose.yml       # Orchestration for Zookeeper, Kafka, and PostgreSQL
├── producer.py              # Ingestion engine simulating real-time order streams
├── stream_processor.py      # Kafka consumer, event validator, and DB persistence
├── dashboard.py             # Streamlit real-time interactive telemetry dashboard
├── requirements.txt         # Project dependencies
└── .gitignore               # Ignored environments and binary artifacts