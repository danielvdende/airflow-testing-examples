import airflow.utils.dates
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator

from airflow.contrib.sensors.wasb_sensor import WasbBlobSensor

dag = DAG(dag_id="azure_blob_sensor", start_date=airflow.utils.dates.days_ago(3), schedule_interval="@daily")


def do_magic(**context):
    print(context)


data_arrival_sensor = WasbBlobSensor(
    task_id="data_arrival_sensor",
    container_name="landing_zone",
    blob_name="raw_data.csv"
)

data_has_arrived = BashOperator(task_id="data_has_arrived", bash_command="echo 'The data has arrived!' && sleep 5m", dag=dag)

data_arrival_sensor >> data_has_arrived
