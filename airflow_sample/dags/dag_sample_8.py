from datetime import timedelta
from typing import Any

import boto3
from airflow import DAG, Dataset
from airflow.decorators import task
from airflow.models.baseoperator import BaseOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.utils.context import Context
from airflow.utils.dates import days_ago
from asgiref.timeout import timeout
from botocore.exceptions import NoCredentialsError
from jinja2.constants import LOREM_IPSUM_WORDS


# BUCKET_NAME = "my-project-dev-mybucketedu4d38caff-mbbhxb34dwj3"
# FILE_KEY = "uploads/file_to_delete.txt"

class CustomPythonOperator(BaseOperator):
  def __init__(self, name: str, task_id: str, **kwargs):
    super().__init__(task_id=task_id, **kwargs)
    self.name = name

  def execute(self, context: Context) -> Any:
    message = f"Hello {self.name}"
    print(message)
    return message


default_args = {
  "owner": "sample_1",
  "depends_on_past": False,
  "start_date": days_ago(1),
  "email": ["sample@sample.com"],
  "email_on_failure": False,
  "email_on_retry": False,
  "retries": 1,
  "retry_delay": timedelta(seconds=10),
  # "queue": "bash_queue",
  # "pool": "backfill",
  # "priority_weight": 10,
  # "end_date": datetime(2016, 1, 1)
}

with DAG(
  dag_id="custom_python_operator",
  default_args=default_args,
  catchup=False
) as dag:

  CustomPythonOperator(task_id="really_symple_task", name="Ivan")

