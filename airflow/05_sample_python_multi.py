from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime


def start():
    print("Start Task")

def end():
    print("End Task")


with DAG(
    dag_id="python_operator_start_end_dag",
    description="Simple PythonOperator DAG with task dependency",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["python", "example"]
) as dag:

    # Task 1
    start_task = PythonOperator(
        task_id="start",
        python_callable=start
    )

    # Task 2
    end_task = PythonOperator(
        task_id="end",
        python_callable=end
    )

    # Task dependency
    start_task >> end_task
