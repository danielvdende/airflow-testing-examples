import airflow.utils.dates
from airflow.models import DAG

from airflow.contrib.sensors.wasb_sensor import WasbBlobSensor

dag = DAG(dag_id="keyvault_connection", start_date=airflow.utils.dates.days_ago(3), schedule_interval="@once")

data_arrival_sensor = WasbBlobSensor(
    task_id="data_arrival_sensor",
    container_name="landing",
    blob_name="raw_data.csv",
    wasb_conn_id="foo",
    poke_interval=60,
    timeout=60*60*24,
    dag=dag
)

