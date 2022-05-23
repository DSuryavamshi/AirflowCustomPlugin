import json
import os
from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults
import requests
import pprint


class DatabricksCustomOperator(BaseOperator):
    @apply_defaults
    def __init__(
        self,
        cluster_policy_id: str,
        job_type: str,
        json: dict,
        notebook_task: dict,
        notebook_params: dict,
        acl_details: list,
        *args,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.cluster_policy_id = cluster_policy_id
        self.job_type = job_type
        self.json = json
        self.notebook_task = notebook_task
        self.notebook_params = notebook_params
        self.acl_details = acl_details
        self.intance_id = "stockx-datalake-stg.cloud.databricks.com"
        self.json = {}

    def _set_json(self, cluster_definition):
        try:
            new_cluster = []
            
            if self.job_type.lower() == "notebook":
                self.json["tasks"][0].update(
                    {"notebook_task": self.notebook_task}
                )
            self.json["access_control_list"] = self.acl_details
        except Exception as e:
            raise Exception(e)

    def _get_cluster_policy(self):
        try:
            api_version = "api/2.0"
            api_command = "policies/clusters/get"
            url = f"https://{self.intance_id}/{api_version}/{api_command}"
            params = {"policy_id": self.cluster_policy_id}
            headers = {
                # "Authorization": f"Bearer {os.environ.get('secret_databricks_stg')}"
                "Authorization": f"Bearer dapi26ec7e4d4aebd724ac82e57b5e4c435e"
            }
            response = requests.get(url=url, params=params, headers=headers)
            print(response.status_code)
            print(type(response))
            print(json.loads(response.json()["definition"]))
            if response.status_code == 200:
                self._set_json(json.loads(response.json()["definition"]))
                print(self.json())
        except Exception as e:
            raise requests.RequestException(f"Error message:\{e}")

    def execute(self, context):
        try:
            api_version = "api/2.1"
            api_command = "jobs/runs/submit"
            url = f"https://{self.intance_id}/{api_version}/{api_command}"
            params = {"policy_id": self.json}
            headers = {
                # "Authorization": f"Bearer {os.environ.get('secret_databricks_stg')}"
                "Authorization": f"Bearer dapia62634f0a08ecad7e0af66696030ab4d",
                "Content-Type": "application/json",
            }
            response = requests.post(
                url=url, json=json.loads(json.dumps(params)), headers=headers
            )
            print(response.json())
        except Exception as e:
            raise Exception(e)


if __name__ == "__main__":

    acl_details = [
        {
            "user_name": "dhanushsuryavamshi@v.stockx.com",
            "permission_level": "CAN_MANAGE",
        }
    ]

    notebook_task = {
        "notebook_path": "/Users/dhanushsuryavamshi@v.stockx.com/test_customOperator",
    }

    opr = DatabricksCustomOperator(
        cluster_policy_id="9C61EF4F960018CD",
        job_type="notebook",
        json={},
        notebook_params={},
        notebook_task=notebook_task,
        acl_details=acl_details,
        task_id="test",
    )
    opr._get_cluster_policy()
    # opr.execute("context")
