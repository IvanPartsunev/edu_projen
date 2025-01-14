from datetime import timedelta

from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago


def generate(ti):
    data = {"data": [1, 2, 3, 4]}
    ti.xcom_push(key="data", value=data)


def process(ti):
    data = ti.xcom_pull(key="data", task_ids="task_1")
    new_data = sum([i for i in data["data"] if i % 2 == 0])
    ti.xcom_push(key="result", value=new_data)

def show_result(ti):
    result = ti.xcom_pull(key="result", task_ids="task_2")
    return result


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
    dag_id = "generate_and_process",
    default_args = default_args,
) as dag:

    task1 = PythonOperator(
        task_id = "task_1",
        python_callable=generate
    )

    task2 = PythonOperator(
        task_id="task_2",
        python_callable=process,
    )

    task3 = PythonOperator(
        task_id="task_3",
        python_callable=show_result,
    )

    task1 >> task2 >> task3