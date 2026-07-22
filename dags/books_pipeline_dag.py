from datetime import datetime

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator


PROJECT_DIR = "/opt/airflow/project"


with DAG(
    dag_id="books_pipeline",
    description="End-to-end books data pipeline",
    start_date=datetime(2026, 7, 22),
    schedule=None,
    catchup=False,
    tags=["Capstone"],
) as dag:

    ingestion = BashOperator(
        task_id="ingestion",
        bash_command=(
            f"cd {PROJECT_DIR} && "
            "echo 'Ingestion stage completed'"
        ),
    )

    delta_lakehouse = BashOperator(
        task_id="delta_lakehouse",
        bash_command=(
            f"cd {PROJECT_DIR} && "
            "python delta_lakehouse.py"
        ),
    )

    quality_gate = BashOperator(
        task_id="quality_gate",
        bash_command=(
            f"cd {PROJECT_DIR} && "
            "python quality_gate.py"
        ),
    )

    rag_pipeline = BashOperator(
        task_id="rag_pipeline",
        bash_command=(
            f"cd {PROJECT_DIR} && "
            "python RAG2.py"
        ),
    )

    pipeline_complete = BashOperator(
        task_id="pipeline_complete",
        bash_command="echo 'Books pipeline completed successfully'",
    )

    (
        ingestion
        >> delta_lakehouse
        >> quality_gate
        >> rag_pipeline
        >> pipeline_complete
    )