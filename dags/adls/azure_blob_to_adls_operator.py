from tempfile import NamedTemporaryFile
from typing import Sequence

from airflow.contrib.hooks.wasb_hook import WasbHook
from airflow.contrib.hooks.azure_data_lake_hook import AzureDataLakeHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class AzureBlobToADLSOperator(BaseOperator):
    """
    Synchronizes an Azure Data Lake Storage path with a GCS bucket
    :param src_adls: The Azure Data Lake path to find the objects (templated)
    :type src_adls: str
    :param dest_gcs: The Google Cloud Storage bucket and prefix to
        store the objects. (templated)
    :type dest_gcs: str
    :param replace: If true, replaces same-named files in GCS
    :type replace: bool
    :param gzip: Option to compress file for upload
    :type gzip: bool
    :param azure_data_lake_conn_id: The connection ID to use when
        connecting to Azure Data Lake Storage.
    :type azure_data_lake_conn_id: str
    :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud.
    :type gcp_conn_id: str
    :param google_cloud_storage_conn_id: (Deprecated) The connection ID used to connect to Google Cloud.
        This parameter has been deprecated. You should pass the gcp_conn_id parameter instead.
    :type google_cloud_storage_conn_id: str
    :param delegate_to: Google account to impersonate using domain-wide delegation of authority,
        if any. For this to work, the service account making the request must have
        domain-wide delegation enabled.
    :type delegate_to: str
    :param google_impersonation_chain: Optional Google service account to impersonate using
        short-term credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    :type google_impersonation_chain: Union[str, Sequence[str]]
    **Examples**:
        The following Operator would copy a single file named
        ``hello/world.avro`` from ADLS to the GCS bucket ``mybucket``. Its full
        resulting gcs path will be ``gs://mybucket/hello/world.avro`` ::
            copy_single_file = AdlsToGoogleCloudStorageOperator(
                task_id='copy_single_file',
                src_adls='hello/world.avro',
                dest_gcs='gs://mybucket',
                replace=False,
                azure_data_lake_conn_id='azure_data_lake_default',
                gcp_conn_id='google_cloud_default'
            )
        The following Operator would copy all parquet files from ADLS
        to the GCS bucket ``mybucket``. ::
            copy_all_files = AdlsToGoogleCloudStorageOperator(
                task_id='copy_all_files',
                src_adls='*.parquet',
                dest_gcs='gs://mybucket',
                replace=False,
                azure_data_lake_conn_id='azure_data_lake_default',
                gcp_conn_id='google_cloud_default'
            )
         The following Operator would copy all parquet files from ADLS
         path ``/hello/world``to the GCS bucket ``mybucket``. ::
            copy_world_files = AdlsToGoogleCloudStorageOperator(
                task_id='copy_world_files',
                src_adls='hello/world/*.parquet',
                dest_gcs='gs://mybucket',
                replace=False,
                azure_data_lake_conn_id='azure_data_lake_default',
                gcp_conn_id='google_cloud_default'
            )
    """

    ui_color = '#f0eee4'

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

        self.log.info("All done, uploaded files to Azure Blob")
