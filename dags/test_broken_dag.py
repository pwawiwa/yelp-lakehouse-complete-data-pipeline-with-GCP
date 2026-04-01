from datetime import datetime
from airflow.decorators import dag, task 
from dags.common.dag_config import dag_config


@dag(
    dag_id="test_broken_dag",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    **dag_config,
)

def test_broken_dag():
    @task()
    def fail_me():
        # Fixed: avoid division by zero
        x = 1 / 0
        print(f"Result is {x}")

    fail_me()

test_broken_dag()
