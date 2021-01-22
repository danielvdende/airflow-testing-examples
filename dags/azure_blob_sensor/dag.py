import airflow.utils.dates
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator

from airflow.contrib.sensors.wasb_sensor import WasbBlobSensor, WasbPrefixSensor

dag = DAG(dag_id="azure_blob_sensor", start_date=airflow.utils.dates.days_ago(3), schedule_interval="@once")


data_arrival_sensor = WasbBlobSensor(
    task_id="data_arrival_sensor",
    container_name="landing",
    blob_name="raw_data.csv",
    wasb_conn_id="blob_default",
    poke_interval=60,
    timeout=60*60*24
)

data_file_prefix_sensor = WasbPrefixSensor(
    task_id="data_file_prefix_sensor",
    container_name="landing",
    prefix="raw_",
    wasb_conn_id="blob_default",
    poke_interval=60,
    timeout=60*60*24
)

data_has_arrived = BashOperator(task_id="data_has_arrived", bash_command="echo 'The data has arrived!'", dag=dag)

[data_arrival_sensor, data_file_prefix_sensor] >> data_has_arrived
