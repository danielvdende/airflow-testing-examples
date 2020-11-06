import airflow.utils.dates
from airflow.models import DAG
import sys
import os

PACKAGE_PARENT = '..'
SCRIPT_DIR = os.path.dirname(os.path.realpath(os.path.join(os.getcwd(), os.path.expanduser(__file__))))
sys.path.append(os.path.normpath(os.path.join(SCRIPT_DIR, PACKAGE_PARENT)))

from datafactory.trigger_data_factory_run_operator import TriggerDataFactoryRunOperator

dag = DAG(dag_id="datafactory_integration", start_date=airflow.utils.dates.days_ago(3), schedule_interval="@daily")
subscription_id="5ddf05c0-b972-44ca-b90a-3e49b5de80dd"

trigger_data_factory_run = TriggerDataFactoryRunOperator(task_id="trigger_data_factory_run",
                                                         dag=dag,
                                                         subscription_id=subscription_id,
                                                         adf_sp_connection_id="azure_data_factory",
                                                         pipeline_name="copyPipeline",
                                                         resource_group_name="abnairflowazure-20631bd75d7412",
                                                         factory_name="adf20631bd75d")
