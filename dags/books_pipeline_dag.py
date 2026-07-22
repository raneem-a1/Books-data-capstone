from datetime import datetime

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator


with DAG(
    dag_id="books_pipeline",
    start_date=datetime(2026, 7, 22),
    schedule=None,
    catchup=False,
    tags=["Capstone"],
) as dag:

    ingestion = BashOperator(
        task_id="ingestion",
        bash_command="echo 'Running Ingestion'",
    )

    delta = BashOperator(
        task_id="delta_lakehouse",
        bash_command="echo 'Running Delta Lakehouse'",
    )

    quality = BashOperator(
        task_id="quality_gate",
        bash_command="echo 'Running Quality Gate'",
    )

    rag = BashOperator(
        task_id="rag_pipeline",
        bash_command="echo 'Running RAG Pipeline'",
    )

    complete = BashOperator(
        task_id="pipeline_complete",
        bash_command="echo 'Pipeline Finished'",
    )

    ingestion >> delta >> quality >> rag >> complete