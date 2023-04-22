"""
Hello, World Upper Case DAG.

Uses the Task Flow API syntax.
"""
from airflow.decorators import dag, task
from datetime import datetime


# Create the DAG
@dag(
    dag_id="hello_world_upper_dag",
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False,
    tags=["tutorial"],
)
def hello_upper():
    @task()
    def generate_message():
        """
        Generate a Hello, World Message, all lowercase
        """
        msg = "hello, world!"
        print(f"Setting message to:  {msg}")
        return msg

    @task()
    def upper_case(msg):
        """
        Convert Message to Upper Case
        """
        print("Calling upper_case")
        print(f"Got message: {msg}")
        upper_msg = msg.upper()
        print(f"Transforming to:  {upper_msg}")

    # Describe the dependencies via nested function calls
    upper_case(generate_message())


# Call the dag function to register the DAG
hello_upper()
