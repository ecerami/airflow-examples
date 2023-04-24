"""
Transfer a file from Google Cloud Storage (GCS) to Big Query.
"""
from airflow.decorators import dag, task
from datetime import datetime
import logging
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
logger = logging.getLogger("airflow.task")


# Create the DAG
@dag(
    dag_id="run_bigquery",
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False,
    tags=["gcp"],
)
def run_big_query():
    """Run a sample Google BigQuery."""

    @task
    def query_iris():
        logger.info("Executing Biq Query")
        bigquery_hook = BigQueryHook(gcp_conn_id="gcp")
        con = bigquery_hook.get_conn()
        cursor = con.cursor()

        # NB:  If your project name includes a dash, you must surround the entire table
        # reference with brackets.
        cursor.execute("select MAX(sepal_length), MAX(sepal_width) from [htan-dashboard-382016.hdash.iris]")
        record = cursor.fetchone()
        logger.info("-------------")
        logger.info(record)
        logger.info("-------------")

    # Call the task function
    query_iris()


# Call the dag function to register the DAG
run_big_query()
