from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def my_function():
    print("Hello from PythonOperator")

with DAG(
    dag_id="python_operator_basic",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False
) as dag:

    task_1 = PythonOperator(
        task_id="run_python",
        python_callable=my_function
    )
