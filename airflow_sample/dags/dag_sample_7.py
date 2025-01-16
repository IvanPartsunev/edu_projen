from datetime import timedelta

import boto3
from airflow import DAG, Dataset
from airflow.decorators import task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.utils.dates import days_ago
from asgiref.timeout import timeout
from botocore.exceptions import NoCredentialsError
from jinja2.constants import LOREM_IPSUM_WORDS


BUCKET_NAME = "my-project-dev-mybucketedu4d38caff-mbbhxb34dwj3"
FILE_KEY = "uploads/file_to_delete.txt"


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
    dag_id="sensors_intro",
    default_args=default_args,
    catchup=False
) as dag:

  check_file = S3KeySensor(
    task_id="check_for_file",
    bucket_name=BUCKET_NAME,
    bucket_key=FILE_KEY,
    poke_interval=10,
    timeout=300,
    mode="poke"   #This is default behaviour, I wrote it only for reminder.
  )

  # @task.sensor(poke_interval=10, timeout=300)
  # def wait_for_file():
  #   s3 = S3Hook()
  #
  #   if s3.check_for_key(FILE_KEY, BUCKET_NAME):
  #     return True
  #   else:
  #     return False

  @task
  def delete_file():
    s3 = S3Hook()
    s3.delete_objects(BUCKET_NAME, FILE_KEY)
    return "File successfully deleted!"

  delete_task = delete_file()
  check_file >> delete_task

