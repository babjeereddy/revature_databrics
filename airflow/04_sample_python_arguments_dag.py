from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

# Python function to be executed
def greet(name):
    print(f"Hello {name}")

# Define the DAG
with DAG(
    dag_id="greet_user_python_operator",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["python_operator", "example"]
) as dag:

    greet_task = PythonOperator(
        task_id="greet_user",
        python_callable=greet,
        op_args=["Shiva"]
    )
