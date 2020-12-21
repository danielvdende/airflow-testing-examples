import airflow.utils.dates
from airflow.models import DAG
import sys
import os

PACKAGE_PARENT = '..'
SCRIPT_DIR = os.path.dirname(os.path.realpath(os.path.join(os.getcwd(), os.path.expanduser(__file__))))
sys.path.append(os.path.normpath(os.path.join(SCRIPT_DIR, PACKAGE_PARENT)))

from adls.azure_blob_to_adls_operator import AzureBlobToADLSOperator

dag = DAG(dag_id="blob_to_adls", start_date=airflow.utils.dates.days_ago(3), schedule_interval="@daily")
subscription_id = "5ddf05c0-b972-44ca-b90a-3e49b5de80dd"

trigger_data_factory_run = AzureBlobToADLSOperator(task_id="copy_blob_to_adls",
                                                   dag=dag,
                                                   src_blob="bar",
                                                   src_blob_container="foo",
                                                   dest_adls="BABABABABAA",
                                                   dest_adls_container="foo",
                                                   azure_data_lake_conn_id="adls_default",
                                                   azure_blob_conn_id="blob_default")
