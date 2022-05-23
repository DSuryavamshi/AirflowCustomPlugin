from .operators.DatabricksCustomOperator import DatabricksCustomOperator
from airflow import DAG

with DAG:
    hello_task = DatabricksCustomOperator(task_id="sample-task", name="foo_bar")
