import airflow
from airflow import DAG
from airflow.contrib.operators.databricks_operator import DatabricksSubmitRunOperator
from airflow import DAG
from airflow_databricks_scala_ranga_sql import queries
from airflow.exceptions import AirflowException, AirflowSkipException
from datetime import datetime, date, timedelta
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from datetime import datetime
import logging


new_cluster_config = {
    "spark_version": "9.1.x-scala2.12",
    "num_workers": 1,
    "node_type_id": "m4.2xlarge",
    "spark_conf": {
        "spark.sql.session.timeZone": "UTC",
        "spark.hadoop.fs.s3a.acl.default": "BucketOwnerFullControl",
        "spark.databricks.hive.metastore.glueCatalog.enabled": "true",
        "spark.sql.adaptive.enabled": "true",
        "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
        "spark.sql.caseSensitive:": "true",
    },
    "cluster_log_conf": {
        "dbfs": {"destination": "dbfs:/FileStore/Ranga/logs/scala_job"}
    },
    "aws_attributes": {
        "instance_profile_arn": "arn:aws:iam::472425883122:instance-profile/stockx-dbx-stg-datalake-delta-instance-profile-rw",
    },
    "disk_spec": {
        "disk_type": {"ebs_volume_type": "GENERAL_PURPOSE_SSD"},
        "disk_count": 1,
        "disk_size": 100,
    },
}

jar_task_params = {
    "new_cluster": new_cluster_config,
    "spark_jar_task": {
        "main_class_name": "com.kwartile.datamorph.DataMorph",
        "access_control_list": [
            {
                "user_name": "ranganathhalagali@v.stockx.com",
                "permission_level": "CAN_VIEW",
            }
        ],
        "parameters": [
            "--datamorph-conf",
            "s3://stockx-analytics-deng-east/deng_dev/Clickstream_Validation_data/configs/daily_run/datamorph_daily_full.conf",
            "--rules",
            "s3://stockx-analytics-deng-east/deng_dev/Clickstream_Validation_data/configs/daily_run/clickstreambronze_dropheight.json",
            "--params",
            "DATE_PARAM=2022-03-30",
        ],
    },
    "libraries": [
        {
            "jar": "dbfs:/FileStore/jars/7d9c91a7_c8af_4b4d_9720_a3af2f168dbe-datamorph_spark_1_1_89_jar_with_dependencies-05e35.jar"
        }
    ],
}


default_args = {
    "owner": "Ranganath Halagali",
    "depends_on_past": False,
    "start_date": datetime(2022, 4, 21),
    "email": ["ranganathhalagali@v.stockx.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 0,
    "sla": timedelta(minutes=20),
}

dag = DAG(
    "Databricks_integration_scala_ranga",
    default_args=default_args,
    max_active_runs=1,
    schedule_interval=None,  # runs every at Mid night.
    description="databricks scala job integration",
    catchup=False,
)

# Below, we are triggering only one class main class
# jar_task = DatabricksSubmitRunOperator(
#     task_id='spark_jar_task',
#     databricks_conn_id='databricks-default',
#     run_name='scala_job',
#     timeout_seconds=3600,
#     dag=dag,
#     json=jar_task_params)

# Here we are tying make each task level split
master_table = "deng.denali_segment_etl_master"
current_date = "{{ds}}"
prev_date = "{{prev_ds}}"
run_id = "{{run_id}}"

datamorph_conf = (
    "s3://stockx-analytics-deng-east/deng_dev/Clickstream_Validation_data/configs/daily_run/"
    "datamorph_daily_full.conf"
)
bronze_rules = (
    "s3://stockx-analytics-deng-east/deng_dev/Clickstream_Validation_data/configs/daily_run/"
    "clickstreambronze_dropheight.json"
)
silver_rules = (
    "s3://stockx-analytics-deng-east/deng_dev/Clickstream_Validation_data/configs/daily_run/"
    "clickstreambronze_dropheight.json"
)

main_class = "com.kwartile.datamorph.DataMorph"
user_name = "ranganathhalagali@v.stockx.com"
jar_path = (
    "dbfs:/FileStore/jars/"
    "7d9c91a7_c8af_4b4d_9720_a3af2f168dbe-datamorph_spark_1_1_89_jar_with_dependencies-05e35.jar"
)

batch_date = "{{ds}}"
prev_batch_date = "{{prev_ds}}"
print("batch_date, prev_batch_date", batch_date, prev_batch_date)

spark_jar_task = DatabricksSubmitRunOperator(
    task_id="spark_jar_task",
    dag=dag,
    databricks_conn_id="databricks_default",
    run_name="scala_job1",
    timeout_seconds=3600,
    new_cluster=new_cluster_config,
    spark_jar_task={
        "main_class_name": main_class,
        "parameters": [
            "--datamorph-conf",
            datamorph_conf,
            "--rules",
            bronze_rules,
            "--params",
            "DATE_PARAM={{ds}}" "DATE_PARAM1={{prev_ds}}",
        ],
        "access_control_list": [
            {"user_name": user_name, "permission_level": "CAN_VIEW"}
        ],
    },
    libraries=[{"jar": jar_path}],
)

spark_jar_task2 = DatabricksSubmitRunOperator(
    task_id="spark_jar_task2",
    dag=dag,
    databricks_conn_id="databricks-default",
    run_name="scala_job2",
    timeout_seconds=3600,
    new_cluster=new_cluster_config,
    spark_jar_task={
        "main_class_name": main_class,
        "parameters": [
            "--datamorph-conf",
            datamorph_conf,
            "--rules",
            bronze_rules,
            "--params",
            "DATE_PARAM={{ds}}",
            "--params",
            "DATE_PARAM1={{prev_ds}}",
        ],
        "access_control_list": [
            {"user_name": user_name, "permission_level": "CAN_VIEW"}
        ],
    },
    libraries=[{"jar": jar_path}],
)

spark_jar_task >> spark_jar_task2
