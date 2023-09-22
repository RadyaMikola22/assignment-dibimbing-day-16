"""
Script ini untuk membuat DAG pyspark_tutorial serta melakukan pembuatan cluster, menjalankan tugas pyspark,
dan menghapus cluster jika sudah tidak digunakan. Adapaun tugas pysparknya merupakan perintah untuk menjalankan
script wordcout pyspark yang telah dipersiapkan sebelumnya.
"""

# Import library yang diperlukan
import datetime
import os
from airflow import models
from airflow.providers.google.cloud.operators import dataproc
from airflow.utils import trigger_rule

# Direktori hasil output dari menjalankan tugas cloud dataproc 
output_dir = (
    os.path.join(
        "{{ var.value.gcs_bucket }}",
        "pyspark_wordcount",
        datetime.datetime.now().strftime("%Y%m%d-%H%M%S"),
    )
    + os.sep
)

# Membuat path ke script PySpark yang tersimpan di GCS
pyspark_script = "gs://dev-fiber-399503/script_wordcount_pyspark.py"

# Argumen untuk meneruskan tugasnya ke PySpark
pyspark_args = [pyspark_script, output_dir]

# Tugas PySpark
PYSPARK_JOB = {
    "reference": {"project_id": "{{ var.value.gcp_project }}"},
    "placement": {"cluster_name": "composer-pyspark-tutorial-cluster-{{ ds_nodash }}"},
    "pyspark_job": {
        "main_python_file_uri": pyspark_script,
        "args": pyspark_args,
    },
}

CLUSTER_CONFIG = {
    "master_config": {"num_instances": 1, "machine_type_uri": "e2-standard-2"},
    "worker_config": {"num_instances": 2, "machine_type_uri": "e2-standard-2"},
}

yesterday = datetime.datetime.combine(
    datetime.datetime.today() - datetime.timedelta(1), datetime.datetime.min.time()
)

default_dag_args = {
    "start_date": yesterday,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": datetime.timedelta(minutes=5),
    "project_id": "{{ var.value.gcp_project }}",
    "region": "{{ var.value.gce_region }}",
}

# [START composer_pyspark_schedule]
with models.DAG(
    "composer_pyspark_tutorial",
    schedule_interval=datetime.timedelta(days=1),
    default_args=default_dag_args,
) as dag:
    # [END composer_pyspark_schedule]

    # Membuat Cloud Dataproc cluster.
    create_dataproc_cluster = dataproc.DataprocCreateClusterOperator(
        task_id="create_dataproc_cluster",
        cluster_name="composer-pyspark-tutorial-cluster-{{ ds_nodash }}",
        cluster_config=CLUSTER_CONFIG,
        region="{{ var.value.gce_region }}",
    )

    # Menjalankan tugas PySpark di dalam master node Cloud Dataproc Cluster.
    run_dataproc_pyspark = dataproc.DataprocSubmitJobOperator(
        task_id="run_dataproc_pyspark", job=PYSPARK_JOB
    )

    # Menghapus Cloud Dataproc cluster.
    delete_dataproc_cluster = dataproc.DataprocDeleteClusterOperator(
        task_id="delete_dataproc_cluster",
        cluster_name="composer-pyspark-tutorial-cluster-{{ ds_nodash }}",
        region="{{ var.value.gce_region }}",
        trigger_rule=trigger_rule.TriggerRule.ALL_DONE,
    )

    # [START composer_pyspark_steps]
    # Menentukan urutan DAG.
    create_dataproc_cluster >> run_dataproc_pyspark >> delete_dataproc_cluster
    # [END composer_pyspark_steps]

# [END composer_pyspark_tutorial]
