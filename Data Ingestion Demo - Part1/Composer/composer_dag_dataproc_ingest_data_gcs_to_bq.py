import os
from airflow import DAG
from datetime import datetime
from airflow import models
from airflow.providers.google.cloud.operators.dataproc \
    import (ClusterGenerator,DataprocCreateClusterOperator,DataprocDeleteClusterOperator,DataprocSubmitJobOperator)



################################ Parameter initializations  ########################################################
DAG_ID = "ingest-sales-data-through-dataproc"
PROJECT_ID = "abc-services-limited"
BUCKET_NAME = "bkt-dataproc-ingest-gcs-to-stg"
CLUSTER_NAME = "dataproc-ingest-sales-data"
REGION = "us-central1"
ZONE = "us-central1-a"

#PySPark scripts paths
SCRIPT_BUCKET_PATH = "abc-ltd-datalake/dataproc-jobs"

# GCS -> STG
SCRIPT_NAME_1 = "1-Ingest-sales-gcs-to-bq_stg.py"

# STG -> RAW
SCRIPT_NAME_2 = "2-move-sales-bq_stg-to-bq_raw.py"

# RAW to Curated
SCRIPT_NAME_3 = "3-move-sales-bq_raw-to-bq_curated.py"

# bq connector (Mandatory)
INIT_FILE = "goog-dataproc-initialization-actions-us-central1/connectors/connectors.sh"

############################## Generating cluster Configurations #######################################################
CLUSTER_GENERATOR_CONFIG = ClusterGenerator(
    project_id=PROJECT_ID,
    zone=ZONE,
    master_machine_type="n1-standard-2",
    worker_machine_type="n1-standard-2",
    master_disk_size=1024,
    worker_disk_size=1024,
    master_disk_type='pd-standard',
    worker_disk_type='pd-standard',
    num_workers=2,
    storage_bucket=BUCKET_NAME,
    init_actions_uris=[f"gs://{INIT_FILE}"],
    metadata={"bigquery-connector-version":"1.2.0","spark-bigquery-connector-version":"0.21.0"}
).make()


############################## PySpark job configs - Start ##############################################################
#------------------- Ingest GCS to STG ----------------------------------------------------------------------------------
PYSPARK_JOB_1 = {
                "reference": {"project_id": PROJECT_ID},
                "placement": {"cluster_name": CLUSTER_NAME},
                "pyspark_job": {"main_python_file_uri": f"gs://{SCRIPT_BUCKET_PATH}/{SCRIPT_NAME_1}"}
                }

#------------------- Ingest STG to RAW ---------------------------------------------------------------------------------
PYSPARK_JOB_2 = {
                "reference": {"project_id": PROJECT_ID},
                "placement": {"cluster_name": CLUSTER_NAME},
                "pyspark_job": {"main_python_file_uri": f"gs://{SCRIPT_BUCKET_PATH}/{SCRIPT_NAME_2}"}
                }
#------------------- Ingest RAW to Curated ------------------------------------------------------------------------------
PYSPARK_JOB_3 = {
                "reference": {"project_id": PROJECT_ID},
                "placement": {"cluster_name": CLUSTER_NAME},
                "pyspark_job": {"main_python_file_uri": f"gs://{SCRIPT_BUCKET_PATH}/{SCRIPT_NAME_3}"}
                }
############################## PySpark job configs - End ##############################################################

############################## DAG definition #########################################################################
with models.DAG(
    DAG_ID,
    schedule="@once",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["composer", "dataproc"],
) as dag:

    ############################ Create cluster with configuration created above ################################################
    create_dataproc_cluster = DataprocCreateClusterOperator(
        task_id="create_dataproc_cluster",
        cluster_name=CLUSTER_NAME,
        project_id=PROJECT_ID,
        region=REGION,
        cluster_config=CLUSTER_GENERATOR_CONFIG,
        dag=dag,
        )

    ########################## PySpark task to ingest sales data to Bigquery - start #################################################
    pyspark_task_gcs_to_bq_stg = DataprocSubmitJobOperator(
        task_id="pyspark_task_gcs_to_bq_stg", 
        job=PYSPARK_JOB_1, 
        region=REGION, 
        project_id=PROJECT_ID,
        dag=dag,
    )
    
    pyspark_task_bq_stg_to_bq_raw = DataprocSubmitJobOperator(
        task_id="pyspark_task_bq_stg_to_bq_raw", 
        job=PYSPARK_JOB_2, 
        region=REGION, 
        project_id=PROJECT_ID,
        dag=dag,
    )
    pyspark_task_bq_raw_to_bq_curated = DataprocSubmitJobOperator(
        task_id="pyspark_task_bq_raw_to_bq_curated", 
        job=PYSPARK_JOB_3, 
        region=REGION, 
        project_id=PROJECT_ID,
        dag=dag,
    )
    ########################## PySpark task to ingest sales data to Bigquery - End #################################################

    ########################## Delete Cluster once done with jobs ##################################################################
    delete_cluster = DataprocDeleteClusterOperator(
        task_id="delete_cluster",
        project_id=PROJECT_ID,
        cluster_name=CLUSTER_NAME,
        region=REGION,
        dag=dag,
    )

    ############### create_dataproc_cluster >> [pyspark_task_bq_to_gcs,pyspark_task_gcs_to_bq] >> delete_cluster

create_dataproc_cluster >> pyspark_task_gcs_to_bq_stg >> pyspark_task_bq_stg_to_bq_raw >> pyspark_task_bq_raw_to_bq_curated >> delete_cluster