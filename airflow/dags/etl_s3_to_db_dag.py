from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime


# Define a simple Python function
def print_hello():
  print("Hello from Airflow!")


# Define the DAG
with DAG(
    dag_id='simple_test_dag',
    default_args={
      'owner': 'airflow',
      'retries': 1,
    },
    description='A simple test DAG to print a message',
    schedule_interval='@daily',  # Run once a day
    start_date=datetime(2024, 1, 9),  # Set a valid start date
    catchup=False,
) as dag:
  # Define a task using PythonOperator
  task_hello = PythonOperator(
    task_id='say_hello',
    python_callable=print_hello,
  )



