import os
import sys
import airflow.utils.dates
from airflow.models import DAG

from airflow.operators.mssql_operator import MsSqlOperator
PACKAGE_PARENT = '..'
SCRIPT_DIR = os.path.dirname(os.path.realpath(os.path.join(os.getcwd(), os.path.expanduser(__file__))))
sys.path.append(os.path.normpath(os.path.join(SCRIPT_DIR, PACKAGE_PARENT)))
from azure_sql.azure_blob_to_mssql_operator import AzureBlobToMsSqlOperator

dag = DAG(dag_id="azure_sql_integration", start_date=airflow.utils.dates.days_ago(3), schedule_interval="@once")

create_table_sql = """
IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name='cars') CREATE TABLE cars(
    id VARCHAR(64) primary key,
    first_name VARCHAR(64),
    last_name VARCHAR(64),
    car_make VARCHAR(64),
    car_model VARCHAR(64),
    gender VARCHAR(64)
);
"""

create_data_table = MsSqlOperator(task_id='create_data_table',
                                  mssql_conn_id="azure_sql",
                                  dag=dag,
                                  sql=create_table_sql,
                                  database="data")

insert_data = AzureBlobToMsSqlOperator(task_id='insert_data',
                                       dag=dag,
                                       src_blob="cars.csv",
                                       src_blob_container="landing",
                                       dest_database="data",
                                       dest_table="cars",
                                       azure_sql_conn_id="azure_sql",
                                       azure_blob_conn_id="blob_default")

create_data_table >> insert_data
