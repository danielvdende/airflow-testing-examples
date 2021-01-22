from tempfile import NamedTemporaryFile
from csv import reader
from airflow.contrib.hooks.wasb_hook import WasbHook
from airflow.hooks.mssql_hook import MsSqlHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class AzureBlobToMsSqlOperator(BaseOperator):
    @apply_defaults
    def __init__(
            self,
            *,
            src_blob: str,
            src_blob_container: str,
            dest_database: str,
            dest_table: str,
            azure_sql_conn_id: str,
            azure_blob_conn_id: str,
            **kwargs,
    ) -> None:
        super().__init__(**kwargs)

        self.azure_blob_conn_id = azure_blob_conn_id
        self.database = dest_database
        self.src_blob = src_blob
        self.src_blob_container = src_blob_container
        self.dest_table = dest_table
        self.azure_sql_conn_id = azure_sql_conn_id

    def execute(self, context):
        source_hook = WasbHook(wasb_conn_id=self.azure_blob_conn_id)

        # Assumption 1: there is sufficient disk space to download the blob in question
        # Assumption 2: The file is a correctly formatted csv file
        with NamedTemporaryFile(mode='a+', delete=True) as f:
            source_hook.get_file(file_path=f.name, container_name=self.src_blob_container, blob_name=self.src_blob)
            f.flush()
            self.log.info("Saving file to %s", f.name)

            csv_reader = reader(f)

            list_of_tuples = list(map(tuple, csv_reader))
            self.log.info(list_of_tuples)

            self.log.info(f"Inserting into {self.dest_table}")
            hook = MsSqlHook(mssql_conn_id=self.azure_sql_conn_id,
                             schema=self.database)
            hook.insert_rows(self.dest_table, list_of_tuples)

        self.log.info(f"Data inserted into {self.database}.{self.dest_table}")
