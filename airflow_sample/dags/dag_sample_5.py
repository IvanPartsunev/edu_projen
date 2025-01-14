from datetime import timedelta

import boto3
from airflow import DAG
from airflow.decorators import task
from airflow.utils.dates import days_ago
from botocore.exceptions import NoCredentialsError


default_args = {
    "owner": "sample_1",
    "depends_on_past": False,
    "start_date": days_ago(1),
    "email": ["sample_1@mail.mail"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1)
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

with DAG(
        dag_id="Taskflow_intro",
        default_args=default_args,
) as dag:
    @task
    def extract() -> dict:
        return {"data": [1, 2, 3, 4]}


    @task
    def transform(data: dict) -> int:
        new_data = sum([i for i in data["data"] if i % 2 == 0])
        return new_data


    @task
    def load(value: int):
        bucket_name = "my-project-dev-mybucketedu4d38caff-mbbhxb34dwj3"
        file_name = "uploads/projen_file.txt"
        file_content = f"Result value is {value}"

        s3 = boto3.client("s3")

        try:
            s3.put_object(Bucket=bucket_name, Key=file_name, Body=file_content)
            print(f"File '{file_name}' successfully uploaded to bucket '{bucket_name}'")

        except NoCredentialsError:
            print("AWS credentials not found. Make sure to configure your credentials.")


    data = extract()
    transformed_data = transform(data)
    load(transformed_data)
