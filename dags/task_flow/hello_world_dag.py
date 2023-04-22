"""
Hello, World DAG.

The simplest possible DAG with exactly one task.

Uses the Task Flow API syntax.
"""
from airflow.decorators import dag, task
from datetime import datetime


# Create the DAG
@dag(
    dag_id="hello_world_dag",
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False,
    tags=["tutorial"],
)
def hello_world():
    @task()
    def generate_message():
        """
        Generate a Hello, World Message, all lowercase
        """
        print("Calling generate_message")
        msg = "hello, world!"
        print(f"Setting message to:  {msg}")

    # Call the task functions
    generate_message()


# Call the dag function to register the DAG
hello_world()
