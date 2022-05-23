from airflow import DAG
from airflow.contrib.operators.databricks_operator import DatabricksSubmitRunOperator
from datetime import datetime, timedelta

new_cluster_config = {
    "spark_version": "10.0.x-cpu-ml-scala2.12",
    "num_workers": 1,
    "node_type_id": "m4.large",
    "aws_attributes": {
        "instance_profile_arn": "arn:aws:iam::472425883122:instance-profile/stockx-dbx-stg-datalake-delta-instance-profile-rw",
    },
    "disk_spec": {
        "disk_type": {"ebs_volume_type": "GENERAL_PURPOSE_SSD"},
        "disk_count": 1,
        "disk_size": 100,
    },
}
json = {
    "access_control_list": [
        {
            "user_name": "deepikajagarlamundi@v.stockx.com",
            "permission_level": "CAN_MANAGE",
        }
    ]
}
spark_python_task = {
    "python_file": "dbfs:/FileStore/Ranga/databricks_ranga.py",
    "parameters": [
        "stockx-data-engineering-production-kafka-audit",
        "denali/segment-denali-events/{{ds}}/",
        "stockx-analytics-deng-east",
        "segment-denali-events/segment_denali_events_updates_lambda_decoded_prod_header/",
    ],
}

notebook_task = {
    "notebook_path": "/Users/dhanushsuryavamshi@v.stockx.com/test_customOperator",
}

default_args = {
    "owner": "Ranganath Halagali",
    "depends_on_past": False,
    "start_date": datetime(2022, 4, 21),
    "email": ["deepikajagarlamundi@v.stockx.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 0,
    "sla": timedelta(minutes=20),
}

with DAG(
    "Databricks_integration_python_acl_test",
    max_active_runs=1,
    schedule_interval=None,
    description="databricks python job integration",
    catchup=True,
    default_args=default_args,
) as dag:

    python_task = DatabricksSubmitRunOperator(
        task_id="python_task",
        databricks_conn_id="databricks_default",
        run_name="integration_sample_code",
        new_cluster=new_cluster_config,
        notebook_task=notebook_task,
        json=json,
    )

    python_task
