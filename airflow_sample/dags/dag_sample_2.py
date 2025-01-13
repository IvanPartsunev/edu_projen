from datetime import timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago


def start():
  return "start"


def process():
  return "process..."


def end():
  return "end!"


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
  "basic_dag",
  default_args=default_args,
  schedule_interval=timedelta(days=1),
) as dag:

  start_ = PythonOperator(
    task_id="start_proc",
    python_callable=start
  )

  process_ = PythonOperator(
    task_id="process_proc",
    python_callable=process
  )

  end_ = PythonOperator(
    task_id="end_proc",
    python_callable=end
  )

  start_ >> process_ >> end_
