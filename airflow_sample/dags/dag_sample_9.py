import random
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
    dag_id="baranching_operator",
    default_args=default_args,
    catchup=False
) as dag:

    @task
    def start_task():
        options = [1, 2]
        result = random.choice(options) % 2 == True
        return result

    @task.branch(task_id="branching")
    def choose_branch(result: bool):
        branch = "first_branch_id" if result else "second_branch_id"
        return branch

    @task(task_id="first_branch_id")
    def first_branch():
        print("You are welcome!")
        return 1

    @task(task_id="second_branch_id")
    def second_branch():
        print("You are NOT welcome!!!")
        return 2

    @task(trigger_rule="none_failed_min_one_success")
    def join(result):
        value = [x for x in result if isinstance(x, int)]

        if value[0] % 2 != 0:
            print("Come in!")
        else:
            print("Go away!")

    start_result = start_task()
    choose_path = choose_branch(start_result)

    first_task = first_branch()
    second_task = second_branch()

    choose_path >> [first_task, second_task]
    join_task = join([first_task, second_task])
    [first_task, second_task] >> join_task

