from datetime import timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

from airflow_sample.plugins.dag_sample_1_funcs import print_welcome, print_date, print_quote

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

dag = DAG(
  "welcome_dag",
  default_args=default_args,
  schedule_interval=timedelta(days=1)
)

print_welcome_task = PythonOperator(
  task_id="print_welcome",
  python_callable=print_welcome,
  dag=dag,
)

print_date_task = PythonOperator(
  task_id="print_date",
  python_callable=print_date,
  dag=dag,
)

print_random_quote = PythonOperator(
  task_id="print_random_quote",
  python_callable=print_quote,
  dag = dag,
)

print_welcome_task >> print_date_task >> print_random_quote