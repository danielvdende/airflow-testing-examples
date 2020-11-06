import airflow.utils.dates
from airflow.models import DAG

from airflow.operators.mssql_operator import MsSqlOperator

dag = DAG(dag_id="azure_sql_integration", start_date=airflow.utils.dates.days_ago(3), schedule_interval="@daily")

create_table_sql = """
IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name='cars') CREATE TABLE 'cars'(
    id INT primary key,
    first_name VARCHAR(64),
    last_name VARCHAR(64),
    car_make VARCHAR(64),
    car_model VARCHAR(64),
    gender VARCHAR(64)
);
GO
"""

create_data_table = MsSqlOperator(task_id='create_data_table',
                                  mssql_conn_id="azure_sql",
                                  dag=dag,
                                  sql=create_table_sql,
                                  database="data")
