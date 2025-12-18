from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

# Default arguments
default_args = {
    "owner": "airflow",
}

# Define DAG
with DAG(
    dag_id="simple_hello_dag",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
) as dag:

    task_1 = BashOperator(
        task_id="print_hello",
        bash_command="echo 'Hello Airflow'",
    )

    task_2 = BashOperator(
        task_id="print_date",
        bash_command="date",
    )

    task_3 = BashOperator(
        task_id="print_date1",
        bash_command="date",
    )

    # Task order
    task_1 >> task_2
    task_1 >> task_3
