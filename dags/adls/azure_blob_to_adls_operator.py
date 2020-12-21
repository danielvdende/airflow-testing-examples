from tempfile import NamedTemporaryFile

from airflow.contrib.hooks.wasb_hook import WasbHook
from airflow.contrib.hooks.azure_data_lake_hook import AzureDataLakeHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class AzureBlobToADLSOperator(BaseOperator):
    @apply_defaults
    def __init__(
            self,
            *,
            src_blob: str,
            src_blob_container: str,
            dest_adls: str,
            dest_adls_container: str = "",
            adls_generation: int = 2,
            azure_data_lake_conn_id: str,
            azure_blob_conn_id: str,
            **kwargs,
    ) -> None:
        super().__init__(**kwargs)

        self.src_blob = src_blob
        self.src_blob_container = src_blob_container
        self.dest_adls = dest_adls
        self.azure_blob_conn_id = azure_blob_conn_id
        self.azure_data_lake_conn_id = azure_data_lake_conn_id
        self.adls_gen = adls_generation
        self.dest_adls_container = dest_adls_container

    def execute(self, context):
        source_hook = WasbHook(wasb_conn_id=self.azure_blob_conn_id)

        # Assumption: there is sufficient disk space to download the blob in question
        with NamedTemporaryFile(mode='wb', delete=True) as f:
            source_hook.get_file(file_path=f.name, container_name=self.src_blob_container, blob_name=self.src_blob)
            f.flush()
            self.log.info("Saving file to %s", f.name)

            if self.adls_gen == 1:
                self.log.info("Uploading to ADLS Gen 1")
                adls_hook = AzureDataLakeHook(azure_data_lake_conn_id=self.azure_data_lake_conn_id)
                adls_hook.upload_file(
                    local_path=f.name, remote_path=f.name
                )
            else:
                self.log.info("Uploading to ADLS Gen 2")
                adls_hook = WasbHook(wasb_conn_id=self.azure_data_lake_conn_id)
                adls_hook.load_file(f.name, container_name=self.dest_adls_container, blob_name=self.dest_adls)

        self.log.info("All done, uploaded files to Azure Data Lake Store")
