# 🚀 Real-Time Crypto & Stock Market Lakehouse

An end-to-end, high-throughput streaming analytics platform designed to ingest raw financial trade ticks, execute ACID-compliant micro-batch processing via Apache Iceberg, and serve sub-second aggregated financial candlesticks (1-minute OHLC bars) to an interactive TradingView dashboard via Trino.

![Architecture](https://img.shields.io/badge/Architecture-Event--Driven%20Lakehouse-blue)
![PySpark](https://img.shields.io/badge/PySpark-3.5.0-orange)
![Apache%20Iceberg](https://img.shields.io/badge/Apache%20Iceberg-1.5.0-blueviolet)
![Trino](https://img.shields.io/badge/Trino-MPP-green)
![FastAPI](https://img.shields.io/badge/FastAPI-0.100+-009688)
![Redpanda](https://img.shields.io/badge/Redpanda-Kafka%20Compatible-red)

---

## 🏗️ Architecture & Data Flow

```text
┌─────────────────┐       JSON Ticks       ┌─────────────────┐
│ Market Producer ├───────────────────────►│ Redpanda Broker │
│  (producer.py)  │      (Port 9092)       │  (Kafka Topic)  │
└─────────────────┘                        └────────┬────────┘
                                                    │ Streaming Read (29092)
                                                    ▼
┌─────────────────┐   Parquet Files        ┌─────────────────┐
│   MinIO Object  │◄───────────────────────┤ PySpark Engine  │
│  Store (S3 API) │                        │ (Structured)    │
└────────┬────────┘                        └────────┬────────┘
         │                                          │ Commit Snapshots
         │                                          ▼
         │                                 ┌─────────────────┐
         └────────────────────────────────►│  Iceberg REST   │
                 Scans Table Files         │     Catalog     │
                                           └────────┬────────┘
                                                    │ Metadata Queries
                                                    ▼
┌─────────────────┐       SQL Queries      ┌─────────────────┐
│ TradingView UI  │◄───────────────────────┤  Trino Engine   │
│ (FastAPI :8000) │                        │  (MPP SQL :8080)│
└─────────────────┘                        └─────────────────┘
