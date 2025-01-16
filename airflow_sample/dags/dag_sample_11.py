from datetime import timedelta, datetime
import random

from airflow import DAG
from airflow.decorators import task
from airflow.example_dags.example_dynamic_task_mapping import added_values
from airflow.utils.dates import days_ago


default_args = {
  "owner": "sample_1",
  "depends_on_past": False,
  "start_date": days_ago(1),
  "email": ["sample_1@email.com"],
  "email_on_failure": False,
  "email_on_retry": False,
  "retries": 1,
  "retry_delay": timedelta(seconds=20),
  # "queue": "bash_queue",
  # "pool": "backfill",
  # "priority_weight": 10,
  # "end_date": datetime(2016, 1, 1)
}

with DAG (
  dag_id="dynamic_task_mapping",
  default_args=default_args,
  catchup=False
) as dag:

  @task
  def generate_inputs():
    number_of_inputs = random.randint(1, 8)

    inputs = [random.randint(1, 20) for _ in range(number_of_inputs)]

    return inputs

  @task
  def add_one_to_all(x):
    return x + 1

  @task
  def sum_values(values):
    total = sum(values)
    print(f"Total sum is: {total}")


  gen_inputs = generate_inputs()
  add_one = add_one_to_all.expand(x=gen_inputs)
  sum_values(add_one)
