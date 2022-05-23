import json
import requests

instance_id = "stockx-datalake-stg.cloud.databricks.com"
user_token = "dapia62634f0a08ecad7e0af66696030ab4d"


api_version = "api/2.0"
api_command = "jobs/runs/submit"
url = f"https://{instance_id}/{api_version}/{api_command}"

headers = {
    "Authorization": f"Bearer {user_token}",
    "Content-Type": "application/json",
}

# params = {
#     "tasks": [
#         {
#             "task_key": "rand_key",
#             "new_cluster": {
#                 "spark_version": "10.0.x-scala2.12",
#                 "node_type_id": "m4.large",
#                 "policy_id": "9C61EF4F960018CD",
#             },
#             "notebook_task": {
#                 "notebook_path": "/Users/dhanushsuryavamshi@v.stockx.com/test_customOperator"
#             },
#         }
#     ],
#     "access_control_list": [
#         {
#             "user_name": "dhanushsuryavamshi@v.stockx.com",
#             "permission_level": "CAN_MANAGE",
#         }
#     ],
# }

params = {
    "tasks": [
        {
            "task_key": "Match",
            "description": "Matches orders with user sessions",
            "new_cluster": {
                "spark_version": "10.0.x-scala2.12",
                "node_type_id": "i3.xlarge",
                "aws_attributes": {"availability": "SPOT", "zone_id": "us-west-2a"},
                "autoscale": {"min_workers": 2, "max_workers": 16},
            },
            "notebook_task": {
                "notebook_path": "/Users/dhanushsuryavamshi@v.stockx.com/test_customOperator"
            },
            "timeout_seconds": 86400,
        }
    ],
    "run_name": "A multitask job run",
    "timeout_seconds": 86400,
    "access_control_list": [
        {
            "user_name": "dhanushsuryavamshi@v.stockx.com",
            "permission_level": "CAN_MANAGE",
        }
    ],
}


# response = requests.get(url=url, params=params, headers=headers)
# print(response.status_code)
# print(type(response))
# print(response)
# if response.status_code == 200:
#     print(response.json()["definition"])

response = requests.post(
    url=url, params=json.loads(json.dumps(params)), headers=headers
)
print(response.json())


# header = {"Authorization": f"Bearer {token}", "accept": "application/json"}
# payload = {"policy_id": "9C61EF4F960018CD"}
# url = "https://stockx-datalake-stg.cloud.databricks.com/2.0/policies/clusters/get"

# resp = requests.get(url=url, headers=header, data=payload)
# print(resp.content)
