"""
Connect to Google Cloud Storage (GCS) Bucket.
"""
from airflow.decorators import dag, task
from datetime import datetime
import logging
from airflow.providers.google.cloud.operators.gcs import GCSListObjectsOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator,
)

logger = logging.getLogger("airflow.task")


# Create the DAG
@dag(
    dag_id="gcs_list",
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False,
    tags=["gcp"],
)
def bucket_list():
    """Get a list of objects in a Google Cloud Storage (GCS) Bucket."""
    gcp_file_list = GCSListObjectsOperator(
        task_id="gcs_list_files", bucket="hdash", gcp_conn_id="gcp"
    )

    @task()
    def output_file_list(file_list):
        """Output File List to the Log."""
        logger.info("Verifying Google Connection")
        logger.info(f"Total number of files:  {len(file_list)}")
        for file in file_list:
            logger.info(file)

    # Call the task functions
    output_file_list(gcp_file_list.output)


# Call the dag function to register the DAG
bucket_list()
