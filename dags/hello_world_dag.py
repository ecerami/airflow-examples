"""
Hello, World DAG.

The simplest possible DAG with exactly one step.

This example illustrates the traditional Python Airflow syntax.
"""
from airflow import DAG
from airflow.operators.python import PythonOperator

from datetime import datetime

def generate_message(ti):
    """
    Generate a Hello, World Message, all lowercase
    """
    print("Calling generate_message")
    msg = "hello, world!"
    print(f"Setting message to:  {msg}")

# Create the DAG
dag = DAG(
    dag_id="hello_world_dag",
    start_date=datetime(2023,1,1),
    schedule=None,
    catchup=False,
    tags= ["tutorial"],
)

t1 = PythonOperator(
    task_id="generate_message",
    python_callable=generate_message,
    dag = dag
)
