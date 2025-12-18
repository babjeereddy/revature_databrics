from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator

DATABRICKS_CONN_ID = "dbx_con"

NOTEBOOK_PATH = "/Workspace/Users/babjee@babjeepranathigmail.onmicrosoft.com/demo.py"  

NEW_CLUSTER = {
    "spark_version": "13.3.x-scala2.12",  
    "node_type_id": "Standard_DS4_v2",    
        "num_workers": 0,
}

with DAG(
    dag_id="run_databricks_notebook",
    start_date=days_ago(1),
    schedule="@daily",
    catchup=False,
    tags=["databricks"],
) as dag:

    run_notebook = DatabricksSubmitRunOperator(
        task_id="submit_databricks_notebook_run",
        databricks_conn_id=DATABRICKS_CONN_ID,
        json={
            "new_cluster": NEW_CLUSTER,
            "notebook_task": {
                "notebook_path": NOTEBOOK_PATH,
                
                
            },
        },
    )

    run_notebook
