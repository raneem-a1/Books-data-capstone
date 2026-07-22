# Books-data-capstone

## Overview
End-to-end data engineering pipeline built for the **Modern Data Engineering for AI Systems** capstone. 
It ingests book metadata, stores it through a Bronze/Silver/Gold Delta Lakehouse, powers a RAG-based 
question-answering system over book descriptions, and is orchestrated end-to-end with automated quality 
gates and lineage tracking.

**Dataset:** [Books Dataset (Kaggle)](https://www.kaggle.com/datasets/saurabhbagchi/books-dataset)

## Problem it solves
Raw, unvalidated book data (from CSV) is streamed, validated, cleaned, aggregated, and made searchable 
via natural language — while every stage is quality-checked, logged, and traceable.

## Architecture
1. **Ingestion** — Kafka producer/consumer with a Pydantic schema gate; malformed records routed to a dead-letter topic.
2. **Delta Lakehouse** — Bronze (raw) → Silver (cleaned/merged) → Gold (aggregated) layers using Delta Lake.
3. **RAG Pipeline** — Chunking, embeddings, vector store, hybrid search (dense + BM25), reranking.
4. **Orchestration** — Airflow DAG coordinating all stages with dependency gating.
5. **Quality Gate + Lineage** — Great Expectations checks + OpenLineage event tracking.

## Prerequisites
- Python 3.10+
- Java (for Kafka + Spark)
- pip

## Setup
```bash
git clone <repo-url>
cd <repo-name>
pip install -r requirements.txt
```


## How to Run
_(will be filled in as each stage is completed)_

## Expected Output
_(will be filled in as each stage is completed)_


## Training Program
Completed as part of the **Modern Data Engineering for AI Systems** program, SDAIA Academy 
(delivered via Learning Space). 
Cohort dates: 19 July – 23 July, 2026

Reference: [SDAIA Academy on GitHub](https://github.com/SDAIAAcademy)


