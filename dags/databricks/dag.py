import airflow.utils.dates
from airflow.contrib.operators.databricks_operator import DatabricksSubmitRunOperator, DatabricksRunNowOperator
from airflow.models import DAG

dag = DAG(dag_id="databricks_integration", start_date=airflow.utils.dates.days_ago(3), schedule_interval="@once")

notebook_job_config = {
    "name": "My_cool_task",
    "new_cluster": {
        "spark_version": "7.3.x-scala2.12",
        "num_workers": 1,
        "node_type_id": "Standard_D3_v2"
    },
    'notebook_task': {
        'notebook_path': '/Users/danielvanderende@godatadriven.com/foo',
    }
}

script_job_config = {
    "name": "My_cool_task",
    "new_cluster": {
        "spark_version": "7.3.x-scala2.12",
        "num_workers": 1,
        "node_type_id": "Standard_D3_v2"
    },
    "spark_python_task": {
        "python_file": "dbfs:/my_job.py"
    }
}

submit_run_databricks_from_notebook = DatabricksSubmitRunOperator(task_id="submit_run_databricks_from_notebook",
                                                                  json=notebook_job_config,
                                                                  dag=dag)

submit_run_databricks_from_script = DatabricksSubmitRunOperator(task_id="submit_run_databricks_from_script",
                                                                json=script_job_config,
                                                                dag=dag)

run_now_databricks = DatabricksRunNowOperator(task_id="run_now_databricks",
                                              job_id=3,
                                              dag=dag)
