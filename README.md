# 🛒 Real-Time E-Commerce Streaming Telemetry & Analytics Pipeline

### JNTUK B.Tech 2nd Year Curriculum Project (Unit 1 & Data Engineering Aligned)

> **Industry Problem Context:**  
> Large-scale e-commerce platforms (like Flipkart, Amazon India) handle millions of concurrent clickstream interactions, flash-sale carts, and checkout transactions every minute. Batch processing systems (traditional daily ETL jobs) cause several hours of latency, leading to lost sales, untracked inventory outages, and delayed fraud detection.  
> This project builds an enterprise-grade, event-driven streaming pipeline that ingests continuous telemetry streams, performs real-time schema validation and ingestion, persists records into an ACID-compliant relational warehouse, and visualizes live Gross Merchandise Value (GMV), stock movements, and transactional throughput on an auto-refreshing dashboard.

---

## 📁 Complete Project Directory Structure

```text
ecommerce-pipeline/
│
├── docker-compose.yml          # Container orchestration (Zookeeper, Apache Kafka, PostgreSQL)
├── producer.py                 # Telemetry ingestion engine simulating real-time order streams
├── stream_processor.py         # Distributed consumer, schema validator, and DB persistence engine
├── dashboard.py                # Streamlit real-time interactive telemetry analytics dashboard
├── requirements.txt            # Python dependencies and connector drivers
├── .gitignore                  # Git tracking exclusion list (venv, bytecode, checkpoints)
└── README.md                   # Complete academic and architectural documentation