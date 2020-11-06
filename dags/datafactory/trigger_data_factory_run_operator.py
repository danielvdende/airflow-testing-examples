import time
from typing import Optional, Dict, Any
from airflow import AirflowException
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from msrestazure.azure_exceptions import CloudError
# import os
# import sys
# sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from .data_factory_hook import AzureDataFactoryHook

BASE_RUN_URL = (
    "https://adf.azure.com/monitoring"
    "/pipelineruns/{run_id}?factory="
    "/subscriptions/{subscription_id}"
    "/resourceGroups/{resource_group}"
    "/providers/Microsoft.DataFactory/factories/{factory_name}"

)


class TriggerDataFactoryRunOperator(BaseOperator):
    template_fields = [
        "resource_group_name",
        "factory_name",
        "pipeline_name",
        "pipeline_parameters",
    ]
    template_ext = ()
    ui_color = "#008ad7"

    @apply_defaults
    def __init__(
            self,
            adf_sp_connection_id: str,
            subscription_id: str,
            resource_group_name: str,
            factory_name: str,
            pipeline_name: str,
            pipeline_parameters: Optional[Dict[str, Any]]={},
            polling_period_seconds=10,
            *args,
            **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.adf_sp_connection_id = adf_sp_connection_id
        self.subscription_id = subscription_id
        self.resource_group_name = resource_group_name
        self.factory_name = factory_name
        self.pipeline_name = pipeline_name
        self.pipeline_parameters = pipeline_parameters or {}
        self.polling_period_seconds = polling_period_seconds

    def execute(self, context):
        hook = AzureDataFactoryHook(self.adf_sp_connection_id)
        try:
            response = hook.client.pipelines.create_run(
                resource_group_name=self.resource_group_name,
                factory_name=self.factory_name,
                pipeline_name=self.pipeline_name,
                parameters=self.pipeline_parameters,
            )
            self.log.info(
                "ADF Pipeline run for pipeline %s has been created with run id %s",
                self.pipeline_name,
                response.run_id,
            )
            self.handle_adf_run(response.run_id, hook)
        except CloudError as e:
            raise AirflowException(e)

    def handle_adf_run(self, run_id: str, hook: AzureDataFactoryHook):
        see_logs = "View run status and logs at {logs}".format(
            logs=BASE_RUN_URL.format(
                run_id=run_id,
                subscription_id=self.subscription_id,
                resource_group=self.resource_group_name,
                factory_name=self.factory_name,
            )
        )
        while True:
            run_state = hook.client.pipeline_runs.get(
                self.resource_group_name, self.factory_name, run_id
            )
            if run_state.status == "Succeeded":
                self.log.info(f"Run with id {run_id} completed successfully.",)
                self.log.info(see_logs)
                return
            elif run_state.status in ["Queued", "InProgress"]:
                self.log.info(f"Run with id {run_id} {run_state.status}...",)
                time.sleep(self.polling_period_seconds)
                continue
            else:
                error_message = "{task} failed with terminal state {state} and message {message}".format(
                    task=self.task_id, state=run_state.status, message=run_state.message
                )
                self.log.info(see_logs)
                raise AirflowException(error_message)