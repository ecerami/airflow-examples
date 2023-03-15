from datetime import datetime

from airflow import DAG
from airflow.decorators import task

# This code is copied/adapted from:
# https://airflow.apache.org/docs/apache-airflow/2.3.0/concepts/dynamic-task-mapping.html

with DAG(dag_id="expand_0",
    start_date=datetime(2023,1,1),
    tags= ["tutorial"],
    schedule=None,
    catchup=False) as dag:

    @task
    def get_number_list():
        return [1,2,3,4,5]

    @task
    def add_one(x: int):
        return x + 1

    @task
    def sum_it(values):
        total = sum(values)
        print(f"Total was {total}")

    num_list = get_number_list()

    # Use expand to dynamically create N add_one tasks
    added_values = add_one.expand(x=num_list)
    sum_it(added_values)