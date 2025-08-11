from __future__ import annotations

import pendulum

from airflow.models.dag import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

with DAG(
    dag_id="spark_pi_calculator_dag2",
    schedule=None,
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    tags=["spark", "example"],
) as dag:
    submit_job = SparkSubmitOperator(
        task_id="submit_spark_pi_job",
        # The connection ID created in the Airflow UI or via env var
        conn_id="spark_conn2",
        # The path to the application file inside the container
        application="local:///opt/bitnami/spark/apps/pi_calculator.py",
        # Arguments to be passed to the Spark application
        application_args=["10"],
        # You can specify other Spark confs here
        conf={"spark.driver.memory": "1g"},
    )
