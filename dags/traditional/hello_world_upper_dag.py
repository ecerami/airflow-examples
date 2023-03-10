"""
Hello, World Upper Case DAG.

Uses the traditional Python Airflow syntax with xcom communication.
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
    ti.xcom_push(key="message", value=msg)

def upper_case(ti):
    """
    Convert Message to Upper Case
    """
    print("Calling upper_case")
    msg = ti.xcom_pull(task_ids='generate_message', key="message")
    print(f"Got message: {msg}")
    upper_msg = msg.upper()
    print(f"Transforming to:  {upper_msg}")

# Create the DAG
dag = DAG(
    dag_id="hello_world_upper_dag",
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

t2 = PythonOperator(
    task_id="upper_case",
    python_callable=upper_case,
    dag = dag
)

t1 >> t2