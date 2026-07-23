# Books Data Engineering for AI Systems Capstone

## Overview
End-to-end data engineering pipeline built for the **Modern Data Engineering for AI Systems** Capstone.

This project processes book metadata through a complete modern data engineering pipeline. It starts with real-time ingestion using Apache Kafka, validates incoming records with Pydantic, stores the data in a Delta Lakehouse (Bronze, Silver, and Gold layers), powers a Retrieval-Augmented Generation (RAG) search system, and orchestrates the entire workflow using Apache Airflow with automated data quality checks and lineage tracking.

**Dataset:** Books.csv

---

## Problem Statement

Book metadata collected from CSV files may contain missing values, invalid records, and inconsistent formats. This project builds a complete pipeline that validates, cleans, stores, and transforms the data into reliable datasets while enabling semantic search and ensuring every stage is monitored, traceable, and quality-controlled.

---

## Project Architecture

### 1. Ingestion
- Apache Kafka Producer
- Apache Kafka Consumer
- Pydantic schema validation
- Dead Letter Queue (DLQ) for invalid records
- Validation error logging

### 2. Delta Lakehouse
- Bronze Layer (Raw Data)
- Silver Layer (Cleaned Data)
- Gold Layer (Business Aggregations)
- Delta MERGE (Upsert)
- Schema Enforcement

### 3. RAG Pipeline
- Document Chunking
- Embeddings Generation
- Vector Database
- Hybrid Search (Dense + BM25)
- Reranking
- Context-aware Question Answering

### 4. Orchestration
- Apache Airflow DAG
- Automated task dependencies
- Pipeline scheduling
- Failure handling

### 5. Quality Gate & Lineage
- Great Expectations
- OpenLineage
- Data validation reports
- Pipeline event tracking

---

## Project Structure

```
BooksProject/
│
├── data/
│   └── Books.csv
├── dags/
│   └── books_pipeline_dag.py
├── producer.py
├── consumer.py
├── dlq_consumer.py
├── schema.py
├── delta_lakehouse.py
├── RAG2.py
├── quality_gate.py
├── docker-compose.yaml
├── requirements.txt
├── README.md
└── .gitignore
```

---

## Technologies

- Python
- Apache Kafka
- kafka-python
- Pandas
- Pydantic
- Apache Spark
- Delta Lake
- Apache Airflow
- Great Expectations
- OpenLineage

---

## Prerequisites

- Python 3.10+
- Java 17
- Apache Kafka
- pip

---

## Installation

```bash
git clone <repository-url>

cd BooksProject

pip install -r requirements.txt
```

---

## How to Run

### Start Kafka

```bash
brew services start kafka
```

### Run Producer

```bash
python3 producer.py
```

### Run Consumer

```bash
python3 consumer.py
```

---

## Current Progress

### Completed

- Kafka Producer
- Kafka Consumer
- Pydantic Schema Validation
- Dead Letter Queue (DLQ)
- Delta Lakehouse
- RAG Pipeline
- Airflow Orchestration
- Great Expectations Quality Gate
- OpenLineage

---

## Expected Output

- Valid records are successfully processed.
- Invalid records are routed to the **books_dlq** topic.
- Validation errors are recorded.
- Clean data is prepared for the Delta Lakehouse.

---

## Training Program

Completed as part of the **Modern Data Engineering for AI Systems** program at **SDAIA Academy**.

**Training Dates:** 19 July 2026 – 23 July 2026

**Reference:** https://github.com/SDAIAAcademy


## Team Members

| Name |
|------|
| Raneem Abdullah Alsheddi |
| Danah musaad Aljumaiah |
| Haya AbdulMajeed Aljuraysi |
