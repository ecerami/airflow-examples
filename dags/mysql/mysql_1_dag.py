"""
MySQL Dag 1.

This code users the MySqlHook to query the database.
Uses the traditional Python Airflow syntax.
"""
from airflow import DAG
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.operators.python import PythonOperator
from datetime import datetime


def fetch_users():
    request = "SELECT * FROM WEB_USER"
    mysql_hook = MySqlHook(mysql_conn_id="mysql", schema="sample")
    connection = mysql_hook.get_conn()
    cursor = connection.cursor()
    cursor.execute(request)

    # Records are output to Airflow Log
    for record in cursor:
        print(record)


# Create the DAG
dag = DAG(
    dag_id="mysql_1",
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False,
    tags=["tutorial"],
)

task = PythonOperator(task_id="fetch_user", python_callable=fetch_users, dag=dag)
