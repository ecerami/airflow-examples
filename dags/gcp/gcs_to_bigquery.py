"""
Transfer a file from Google Cloud Storage (GCS) to Big Query.
"""
from airflow.decorators import dag, task
from datetime import datetime
import logging
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator,
)
logger = logging.getLogger("airflow.task")


# Create the DAG
@dag(
    dag_id="gcs_to_bigquery",
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False,
    tags=["gcp"],
)
def transfer_to_bigquery():
    """Transfer a file from Google Cloud Storage (GCS) to BigQuery."""
    gcp_transfer = GCSToBigQueryOperator(
        task_id="gcs_to_bigquery",
        bucket="hdash",
        source_objects="iris.csv",
        destination_project_dataset_table="htan-dashboard-382016.hdash.iris",
        autodetect=True,
        gcp_conn_id="gcp",
    )

    gcp_transfer

# Call the dag function to register the DAG
transfer_to_bigquery()
