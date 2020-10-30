#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
import time
from typing import Any

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from datafactory.data_factory_hook import AzureDataFactoryHook


class TriggerDataFactoryRunOperator(BaseOperator):
    """
    Deletes blob(s) on Azure Blob Storage.
    :param container_name: Name of the container. (templated)
    :type container_name: str
    :param blob_name: Name of the blob. (templated)
    :type blob_name: str
    :param wasb_conn_id: Reference to the wasb connection.
    :type wasb_conn_id: str
    :param check_options: Optional keyword arguments that
        `WasbHook.check_for_blob()` takes.
    :param is_prefix: If blob_name is a prefix, delete all files matching prefix.
    :type is_prefix: bool
    :param ignore_if_missing: if True, then return success even if the
        blob does not exist.
    :type ignore_if_missing: bool
    """

    @apply_defaults
    def __init__(
            self,
            *,
            pipeline_name: str = "",
            factory_name: str = "",
            resource_group_name: str = "",
            azure_conn_id: str = 'azure_default',
            **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.azure_conn_id = azure_conn_id
        self.pipeline_name = pipeline_name
        self.factory_name = factory_name
        self.resource_group_name = resource_group_name

    def execute(self, context: dict) -> None:
        self.log.info(f"Running pipeline {self.pipeline_name} in factory {self.factory_name}")
        hook = AzureDataFactoryHook(conn_id=self.azure_conn_id)


        run_response = hook.run_pipeline(self.pipeline_name, resource_group_name=self.resource_group_name, factory_name=self.factory_name)

        # Monitor the pipeline run
        status = hook.get_pipeline_run(run_id=run_response.run_id, resource_group_name=self.resource_group_name, factory_name=self.factory_name).status
        self.log.info(status)
        while status == "InProgress" or status == "Queued":
            time.sleep(1)
            status = hook.get_pipeline_run(run_id=run_response.run_id, resource_group_name=self.resource_group_name, factory_name=self.factory_name).status
            self.log.info("pipeline still running")
            self.log.info(status)

        status = hook.get_pipeline_run(run_id=run_response.run_id, resource_group_name=self.resource_group_name, factory_name=self.factory_name).status
        self.log.info(status)
        if status != "Succeeded":
            self.log.info("FAILFAILFAIL")
            exit(1)
