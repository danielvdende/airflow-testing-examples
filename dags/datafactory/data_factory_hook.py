from airflow.hooks.base_hook import BaseHook
from azure.identity import ClientSecretCredential
from azure.mgmt.datafactory import DataFactoryManagementClient


class AzureDataFactoryHook(BaseHook):
    """
    The connection needs:
      login: the clientId of the service principal
      password: the secret of the service principal
      tenant_id: the tenant id of the service principal
      subscription: the subscription id of the service principal
    """

    def __init__(self, conn_id="azure_data_factory"):
        self.conn_id = conn_id
        self.conn = self.get_connection(self.conn_id)
        self.subscription_id = self.conn.extra_dejson["subscription_id"]
        self.tenant_id = self.conn.extra_dejson["tenant_id"]
        self.client = self.get_conn()

    def get_conn(self) -> DataFactoryManagementClient:
        credentials = ClientSecretCredential(
            client_id=self.conn.login, client_secret=self.conn.password, tenant_id=self.tenant_id
        )
        return DataFactoryManagementClient(credentials, self.subscription_id)
