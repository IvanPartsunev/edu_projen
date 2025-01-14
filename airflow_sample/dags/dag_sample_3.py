from datetime import timedelta

from airflow import DAG
from airflow.decorators import task
from airflow.utils.dates import days_ago

default_args = {
  "owner": "sample_1",
  "depends_on_past": False,
  "start_date": days_ago(1),
  "email": ["sample_1@mail.mail"],
  "email_on_failure": False,
  "email_on_retry": False,
  "retries": 1,
  "retry_delay": timedelta(minutes=5)
  # 'queue': 'bash_queue',
  # 'pool': 'backfill',
  # 'priority_weight': 10,
  # 'end_date': datetime(2016, 1, 1),
}

with DAG(
  dag_id="dynamic_generation",
  default_args=default_args
) as dag:

  @task
  def task1(x: int) -> str:
    return f"Task {x}"

  @task
  def task2(tasks) -> str:
    concat_str = ""

    for current_task in tasks:
      concat_str += current_task

    return concat_str

  generated_tasks = task1.expand(x=[1, 2, 3])
  final_result = task2(generated_tasks)
