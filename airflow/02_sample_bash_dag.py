from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {"owner": "airflow"}

with DAG(
    dag_id="simple_hello_dag1",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
) as dag:

    show_info = BashOperator(
        task_id="show_system_info",
        bash_command="""
echo "Current directory:"
pwd
echo "Date:"
date
echo "Files:"
ls -l
""",
    )
