from datetime import timedelta

import boto3
from airflow import DAG, Dataset
from airflow.decorators import task
from airflow.utils.dates import days_ago
from botocore.exceptions import NoCredentialsError
from jinja2.constants import LOREM_IPSUM_WORDS


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

dataset = Dataset("s3://my-project-dev-mybucketedu4d38caff-mbbhxb34dwj3/uploads/dataset.txt")

with DAG(
    dag_id="dataset_intro_producer",
    default_args=default_args,
    catchup=False
) as dag_1:

  @task
  def produce_file():
    path = "/tmp/dataset_file.txt"

    with open(path, "w") as file:
      file.write(LOREM_IPSUM_WORDS)
      print("File created!")

    return path

  @task(outlets=[dataset])
  def upload_file_to_s3(path: str):
    bucket_name = "my-project-dev-mybucketedu4d38caff-mbbhxb34dwj3"
    file_name = "loads/dataset.txt"

    s3 = boto3.client("s3")

    try:
      s3.upload_file(path, Bucket=bucket_name, Key=file_name)
      print(f"File '{file_name}' successfully uploaded to bucket '{bucket_name}'")

    except NoCredentialsError:
      print("AWS credentials not found. Make sure to configure your credentials.")

  file_path = produce_file()
  upload_file_to_s3(file_path)

with DAG(
    dag_id="dataset_intro_consumer_dag",
    default_args=default_args,
    schedule=[dataset],
    catchup=False
) as dag_2:

  @task
  def download_and_process():
    s3 = boto3.client("s3")
    bucket_name = "my-project-dev-mybucketedu4d38caff-mbbhxb34dwj3"
    key = "loads/dataset.txt"
    new_key = "uploads/edited_dataset.txt"

    response = s3.get_object(Bucket=bucket_name, Key=key)
    file_content = response["Body"].read().decode("utf-8")
    new_file_content = f"New content: {file_content}"

    try:
      s3.put_object(Bucket=bucket_name, Key=new_key, Body=new_file_content)
      print(f"File '{new_key}' successfully uploaded to bucket '{bucket_name}'")
    except NoCredentialsError:
      print("AWS credentials not found. Make sure to configure your credentials.")

  download_and_process()

