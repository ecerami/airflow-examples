"""
MySQL Dag 0.

Uses the traditional Python Airflow syntax.
"""
from airflow import DAG
from airflow.providers.mysql.operators.mysql import MySqlOperator

from datetime import datetime

# Create the DAG
dag = DAG(
    dag_id="mysql_0",
    start_date=datetime(2023,1,1),
    schedule=None,
    catchup=False,
    tags= ["tutorial"],
)

mysql_task = MySqlOperator(
    task_id="insert_record",
    mysql_conn_id = "mysql",
    sql="insert into WEB_USER (FIRST_NAME, LAST_NAME, EMAIL) "
        + "values ('Ethan', 'Cerami', 'cerami@gmail.com');",
    dag=dag
)


