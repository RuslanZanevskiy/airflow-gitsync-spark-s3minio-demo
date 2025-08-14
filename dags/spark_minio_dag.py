import pendulum
from airflow.models.dag import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

# --- 1. S3 Connection Details ---
# It's a good practice to retrieve connection details using Airflow Hooks.
# This avoids hardcoding credentials in the DAG file.
# The 's3_default' connection is created automatically by the docker-compose.yml file.
s3_hook = S3Hook(aws_conn_id='s3minio_conn')
s3_connection = s3_hook.get_connection('s3minio_conn')

# Extract the endpoint URL and credentials from the Airflow connection
minio_endpoint_url = s3_connection.extra_dejson.get('endpoint_url')
minio_access_key = s3_connection.login
minio_secret_key = s3_connection.password

print(s3_connection.extra_dejson)
print(minio_endpoint_url)

# --- 2. Default Arguments ---
# These are the default parameters for the DAG and its tasks.
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
}

# --- 3. DAG Definition ---
# Instantiate the DAG
with DAG(
    dag_id='spark_minio_dag',
    default_args=default_args,
    description='A simple DAG to run a Spark job that interacts with MinIO',
    schedule=None,  # This DAG runs only when manually triggered
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    tags=['spark', 'minio', 'example'],
) as dag:

    # --- 4. Task Definition ---
    # Define the Spark job submission task.
    submit_spark_job = SparkSubmitOperator(
        task_id='submit_spark_job',
        conn_id='spark_conn',
        application='/opt/bitnami/spark/apps/process_data.py',
        verbose=True,
        packages="org.apache.hadoop:hadoop-aws:3.4.1,com.amazonaws:aws-java-sdk-bundle:1.12.720,software.amazon.awssdk:bundle:2.32.20",
        conf={
            "spark.hadoop.fs.s3a.aws.credentials.provider": "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
            "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
            "spark.hadoop.fs.s3a.endpoint": minio_endpoint_url,
            "spark.hadoop.fs.s3a.access.key": minio_access_key,
            "spark.hadoop.fs.s3a.secret.key": minio_secret_key,
            "spark.hadoop.fs.s3a.path.style.access": "true",
            "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
        }
    )

